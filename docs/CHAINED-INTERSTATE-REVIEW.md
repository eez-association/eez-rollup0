# Chained-interstate change review

> A hunk-by-hunk walkthrough of everything in this change-set (staged +
> unstaged working tree vs `2c6ad3d`), written for review. Each section
> covers one file group in file order; every hunk is accounted for.
> Design rationale lives in `docs/CHAINED-INTERSTATE-DESIGN.md`; this
> document explains the *code*.

## The change in one paragraph

Issue #88: the composer simulated each held cross-chain tx in isolation
against the same pre-slot state. Three co-bundled `increment()` calls all
recorded `returnData = 1`; real execution returns 1, 2, 3; the second
delivery reverts `RollingHashMismatch` on-chain; the proof signer rejects
the whole window; the txs re-queue at the front and the composer loops
forever. The fix makes composition *chained*: the Sync block under
construction **is** the L2 simulation state (`SyncBlockState`), a slot-pinned
L1 world accumulates surviving effects, claims are read off executions of
the **real contract paths** (a probe of the canonical delivery tx; real
`EEZ → proxy.executeOnBehalf` frames), and every accepted tx is appended
to the real block and receipt-checked immediately — so the on-chain
compare that used to freeze the composer now costs a single tx an
eviction at compose time.

## Reading map

| Part | Files | What it covers |
|---|---|---|
| 1 | `eez-composer/src/local/build.rs`; `eez-protocol/src/system_tx.rs`, `abi.rs`; `Cargo.toml` | `SyncBlockState`/`SyncBlockFork` (block-is-the-session) + equivalence tests; canonical-builder factoring; new ABI surface |
| 2 | `eez-composer/src/local/slot.rs` (new), `client.rs`, `mod.rs`, `lib.rs` | The two real-path executors, probe inspector, source-sim seams |
| 3 | `eez-composer/src/composer.rs` | The drain rework: two-phase canonical order, accept/evict protocol, keystone assert |
| 4 | tests, harness, `main.rs`, CI, compose file, `Counter.sol` | Verification + wiring + operational knobs |
| A | — | Incidents from live chiado testing (why three late hunks exist) |
| B | — | Headline behavior changes (each part ends with its full inventory) |

Parts are in dependency order — the drain (Part 3) consumes everything
before it.

## What deliberately did NOT change

- Entry building, `prepare_post_batch_raw`, `sync_block_pair_roots`, the
  deriver, the proof signer, the optimistic observer, held-pool
  semantics, and the overlay/nested re-entry machinery are untouched.
- The old `LocalExecutionSession` (direct-call shortcut) still exists in
  `session.rs`; it is simply no longer on the claim path.
- All existing tests pass unmodified (522 workspace tests; the full e2e
  matrix re-verified after every step of this change).



---

# Part 1 — Execution foundations and protocol helpers


The design doc's §2 claim is "the Sync block is the session": during
composition, "L2 state right now" must be *exactly* the state the real block
will have at that position — because every claim recorded during composition
is later checked by executing that same block, on-chain and at the proof
signer. These four files are where that claim is made true. `build.rs` grows
the live prefix state, `system_tx.rs` makes the composer's incremental block
and the canonical rebuild share one code path, `abi.rs` teaches the workspace
the manager/proxy function signatures the new L1 executor drives, and the
manifests pay for a mock provider so the equivalence can be tested at all.

Verified while reviewing: `cargo test -p eez-composer --lib local::build` —
3 passed.

## `crates/eez-composer/src/local/build.rs`

620 lines added, ~90 moved. Roughly a third is the new live-state type, a
third is helper extraction, and a third is tests.

### Hunk 1 — module doc

Three lines saying what the file now is: the same block construction, also
exposed as a *live* incrementally extended state, so "a claim read off it is
the value the committed block produces." Accurate framing; the rest of the
file delivers it.

### Hunk 2 — imports and the `DraftDb` alias

Mostly mechanical import growth (`Evm as _`, `EvmEnvFor`, `InspectorFor`,
`Recovered`, `StateProviderBox`, `CacheState`, `NoOpInspector`,
`DatabaseCommit`, `ExecutionResult`, `Arc`). The one thing worth reading is
the new public alias:

```rust
pub type DraftDb = State<StateProviderDatabase<StateProviderBox>>;
```

Note the `StateProviderBox`: `build_sync_block` builds its `State` over a
*borrowed* `&dyn StateProvider` (it must keep the provider alive separately
to hand to `finish`), whereas the live path owns a boxed provider so the
state can outlive the call that opened it. Same database, different
ownership — the reason for the alias, and for `open_draft_db` existing
alongside the inline builder in `build_sync_block`.

### Hunk 3a — `BuiltSyncBlock.tx_successes`

**Before:** the final build returned payload + header + block. Whether any tx
in it *reverted* was invisible to the caller.

**After:** a `Vec<bool>` of receipt statuses in block order.

This is the belt half of the belt-and-braces. Each tx was already
receipt-checked when it was appended to the live prefix; `tx_successes` lets
the composer re-check the same thing on the final, independently built block
(`composer.rs:2416`) and bail to a minimal postBatch if the two ever
disagree. Given the whole point of the change is that appended state ==
final-block state, this field is the assertion that says so out loud rather
than trusting it.

### Hunk 3b — `next_block_attributes` extracted

The `NextBlockEnvAttributes` literal (timestamp, fee recipient,
`prev_randao = 0`, `BUILDER_GAS_LIMIT`, cancun-gated
`parent_beacon_block_root`, shanghai-gated `withdrawals`, `extra_data`) moves
out of `build_sync_block` into a free function, comment and all.

Pure extraction — no field changed. But it is the single most important
extraction in the diff: `SyncBlockState::open` and `build_sync_block` now
*cannot* disagree about the block env. If they could, the divergence would be
silent and nasty — e.g. `SyncBlockState` computing a claim under one timestamp
while the committed block executes under another, so `block.timestamp` reads
differently and a return value the entry hash commits to changes. Sharing the
constructor makes that class of bug unrepresentable instead of merely
unlikely.

### Hunk 3c — `recover_tx` extracted

Decode-2718 + signer recovery, previously inline in `build_sync_block`'s
loop, now a helper taking the block position for error labelling. Mechanical.

### Hunk 3d — `open_draft_db`

New helper: `state_by_block_hash(parent)` → `State::builder()` with bundle
updates, plus an optional `with_cached_prestate(cache)`. The `Option<CacheState>`
is the fork seam — `None` for a fresh prefix, `Some(clone)` for a fork. Note
this is the same `with_cached_prestate` mechanism the overlay/nested path
already relies on in production, so it is not a new trust assumption.

### Hunk 4 — `build_sync_block` uses the helpers

Three small edits: `attributes` now comes from `next_block_attributes(...)`;
the tx loop is `builder.execute_transaction(recover_tx(tx_bytes, idx)?)`;
after `finish`, receipts are mapped into `tx_successes`. No behavior change
beyond the new field being populated.

### Hunk 5 — `TxOutcome` and `exec_outcome`

```rust
pub struct TxOutcome { pub success: bool, pub gas_used: u64, pub output: Bytes }
```

The distinction encoded here drives the entire drain's evict-vs-degrade
policy, so it is worth stating precisely: **a revert is a successful
execution** — `Ok(TxOutcome { success: false, output: <revert data> })`.
Only a tx revm refuses outright (bad nonce, insufficient funds, undecodable)
comes back as `Err(BuildError)`.

Concretely, in the drain (`composer.rs::append_and_execute`) `Ok(!success)` means
"this tx's own claim contradicts its execution → evict this tx, keep
composing" while `Err` means "we couldn't even run it". Collapsing the two
would either silently drop revert data (which the probe needs for the rolling
hash) or turn a single bad tx into a whole-slot failure.

`gas_used` is `result.tx_gas_used()` — gas after refunds, exactly what the
receipt reports and what the header's `gas_used` accumulates. That identity is
what makes the tests below work. `output` is `None → empty` for a halt, as
documented.

### Hunk 6 — `execute_and_commit_inspected`

The per-tx engine for the live paths: `recover_tx`, build an EVM over the
state with the block's env, `transact(&recovered)`, snapshot the outcome,
`db_mut().commit(result.state)`.

The load-bearing question is whether this really equals what
`BlockBuilder::execute_transaction` does to *state*. I checked the pinned
`alloy-evm` (0.34.0, `src/eth/block.rs`):

- `execute_transaction_without_commit` = a block-gas-availability precheck,
  then `self.evm.transact(tx_env)` — same call.
- `commit_transaction` = `system_caller.on_state(...)` (a notification hook,
  no-op unless a state hook was installed — the block builder installs none
  here), gas counters, `receipts.push(build_receipt(...))`, then
  `self.evm.db_mut().commit(state)` — same commit.

So the claim holds: everything the builder does beyond this is receipts and
block-gas bookkeeping, neither of which touches state. The doc comment says
exactly that, including that cumulative block gas is deliberately *not*
tracked here and the final `build_sync_block` is what enforces it.

Two honest footnotes:

1. The comment says the uninspected path "passes `NoOpInspector`, the same
   inspector reth uses when a caller supplies none." True at the API level,
   but not literally the same construction: reth's `evm_with_env` →
   `EthEvmFactory::create_evm` → `EthEvmBuilder::new(db, env).build()` leaves
   `inspect: false`, while `evm_with_env_and_inspector` calls
   `activate_inspector`, setting `inspect: true` and taking revm's
   inspector-enabled loop. With `NoOpInspector` the hooks do nothing, so
   outcomes are identical — and the tests below cross precisely this boundary
   (builder-executed prefix vs `transact`-executed tail, compared on gas), so
   it is pinned rather than assumed. No change needed; just don't read the
   comment as "identical construction."
2. Skipping the block-gas precheck is deliberate and documented, but it does
   move *where* an over-budget window fails: not per-tx at append time, but
   at the final `build_sync_block`, which the composer treats as systemic
   (re-queue survivors + degrade to minimal). A drain that overshoots
   `BUILDER_GAS_LIMIT` (30M) would therefore re-drain the same set next slot
   and fail the same way — the §8-finding-2 shape one layer down. Remote at
   today's bundle caps (~24 txs × ~300k), but a cumulative-gas counter on
   `SyncBlockState` that evicts the offending tx would close it. Worth a
   follow-up line, not a blocker.

### Hunk 7 — `SyncBlockState`

The centerpiece: provider + evm config + parent hash + the block's `evm_env` +
the live `State` + `applied` (the count of txs applied, i.e. the next tx's
block position; used for error labelling and to seed forks). Hand-written
`Debug` because `State` isn't `Debug`.

`open()` is where the §2 guarantee is actually established, and the sequencing
matters:

```rust
let mut builder = evm_config.builder_for_next_block(&mut state, parent, attributes)?;
builder.apply_pre_execution_changes()?;
for (idx, raw) in prefix_txs.iter().enumerate() {
    builder.execute_transaction(recover_tx(raw, idx)?)?;
}
// scope ends — builder dropped, `finish` NEVER called
```

Three things to notice.

*The prefix runs through the real `BlockBuilder`*, not through the cheaper
`transact` path — so pre-execution changes (EIP-2935 block-hash write, beacon
root) are applied exactly once, by the same code that applies them in the
committed block.

*The builder is dropped without `finish()`.* This is the subtle, correct call.
`finish` applies post-execution changes (withdrawals, balance increments) and
computes the state root. Mid-block state must not include those: tx k+1 in a
real block runs after tx k, **not** after block close. Calling `finish` here
would produce a state that no position in the real block ever has. Because the
builder holds `&mut state`, the commits it made stay behind when it drops —
that is what leaves `state` sitting at the mid-block point.

*The stored `evm_env` is the builder's env.* I checked reth's
`builder_for_next_block` (fd59fd2, `crates/evm/evm/src/lib.rs:410`): it does
`let evm_env = self.next_evm_env(parent, &attributes)?` and builds its EVM
from that. `open` computes `next_evm_env(parent, &attributes)` from the same
attributes, so the env `execute_tx` uses later is byte-for-byte the env the
prefix ran under. Good.

`execute_tx()` appends one tx via `execute_and_commit_inspected` with
`NoOpInspector` and bumps `applied`. `fork()` opens a fresh `State` over the
same parent provider, preloaded with a *clone* of the live cache, carrying the
same env and `applied` cursor.

Two small notes: `fork(&mut self)` only needs `&self` (it clones
`self.state.cache`) — harmless, but a `&self` signature would document
"forking cannot disturb the block" in the type. And `open` always re-executes
the whole prefix from the parent, so an eviction-and-reopen cycle is O(n²) in
a drain; that is a conscious trade, and `composer.rs:1647` says why —
rebuilding from the accepted list, rather than restoring a cache, is what
keeps the prefix *provably* equal to the block the canonical rebuild
produces. At a 24-tx cap that is the right side of the trade.

### Hunk 8 — `SyncBlockFork`

A throwaway copy: same fields minus the provider (it can't re-open, by
construction). `execute_tx` mirrors `SyncBlockState`'s;
`execute_tx_inspected(raw, inspector)` is the probe path, where the inspector
captures the inner `EEZL2 → proxy` frame outcome that becomes the claim.
`cache_snapshot()` / `restore_cache()` are the restore point the composition
builder's revert-span rollback needs — the comment correctly justifies why the
cache alone suffices (forks never merge transitions, so there is no
`transition_state` to unwind). `state_and_env()` hands out the raw state and
env for callers that drive their own EVM — used by the source sim at
`composer.rs:1797`.

The invariant this type carries is simply: nothing executed on a fork touches
the block. Probes and source sims live here; only accepted effects get
appended to the real `SyncBlockState`.

### Hunk 9 — `sync_block_pair_roots` untouched

Worth calling out that it is *correctly* left alone. It needs state **roots**,
which only exist after block close — and `SyncBlockState` deliberately never
closes a block. So its build-per-prefix loop can't be swapped for the cheaper
primitive; the two serve different questions ("what is the state mid-block"
vs. "what is the root at this pair-end").

### Hunk 10 — tests

Three tests, plus a fixture. They matter more than usual here, because the
whole design rests on "the live path executes like the builder does," and that
equivalence is otherwise invisible.

The mechanism is worth understanding. `MockEthProvider` (the new dev-dep) is a
flat in-memory account store — I confirmed `state_by_block_hash` ignores the
hash and returns the same provider — so state *roots* it produces are
meaningless. The tests therefore use **per-tx gas as the witness**:

```rust
fn builder_gas(&self) -> Vec<u64> {
    let cumulative = (0..=self.txs.len()).map(|k| self.build(&self.txs[..k]).header.gas_used());
    cumulative.windows(2).map(|w| w[1] - w[0]).collect()
}
```

i.e. build the block on every prefix and difference the header's `gas_used`,
yielding exactly what the *builder* charged each tx. Gas is a state-dependent
signal: the fixture's `STORE` contract does `SSTORE 1 → slot 0`, so a cold
first write costs ~20k more than the warm repeat. Equal per-tx gas across the
two paths therefore implies equal intermediate state, not just equal final
answers. The fixture's 5 txs (transfer, store, reverter, store again,
transfer) exercise plain transfer, state write, revert-with-data, warm rewrite,
and a post-revert tx.

- `prefix_state_execution_matches_build_sync_block` — appends all five to a
  live `SyncBlockState` and compares status + gas against the built block, tx by
  tx. Also asserts the revert carries `0xdeadbeef` and the successful store
  returns 42. Anti-vacuity: `outcomes[1].gas_used > outcomes[3].gas_used +
  15_000` — the second store is cheap *only* if the first one's write is
  visible, so the test fails if `open` ever restarts from the parent. Plus a
  `builder_gas[0] == 21_000` ground-truth assert so the comparisons can't pass
  on empty numbers.
- `prefix_open_matches_the_same_position_in_the_block` — for every k, opens
  the prefix `[0..k]` (builder-executed) and runs tx k through `execute_tx`
  (`transact`-executed), checking it reproduces tx k's in-block outcome. This
  is the one that pins the *seam* between the two execution mechanisms,
  including the `inspect: true` vs `inspect: false` difference noted above.
- `fork_is_isolated_and_cache_restore_rewinds` — a tx run on a fork produces
  the same outcome when subsequently run on the block (so the fork wrote
  nothing back), and `cache_snapshot` / `restore_cache` is a genuine restore
  point (same tx, run → restore → re-run, identical outcome). Anti-vacuity is
  the sharpest of the three: replaying a third time *without* a rewind must
  `Err`, because the nonce is spent. That is precisely the property the
  probe's snapshot/restore in `slot.rs` depends on — the replay is legal only
  because the restore really rewound.

Honest limitation: because the mock returns the same state for any block hash,
these tests verify execution equivalence, not parent-hash routing. That is the
right scope — hash routing is covered by the e2e tests that run real nodes.

## `crates/eez-protocol/src/system_tx.rs`

Two factorings. Neither changes what gets built on the happy path; both remove
a place where two copies of one rule could drift apart.

### Hunk 1 — module doc

`simulate_and_resolve` → "chained simulation". One word; the old name no
longer describes the path.

### Hunk 2 — `build_inbound_system_txs` calls the shared predicate

**Before:** an inline `if !entry.success || entry.l2ToL1Calls.len() != 1 || …`
block — a *third* hand-rolled copy of the same protocol rule (the closure in
`build_cross_chain_sync_pairs` was the second).

**After:** `check_entry_shape(entry, "inbound")?`.

Three copies of one predicate is a real drift hazard: change the rule in one,
and the composer accepts a shape the deriver rejects (or vice versa) — a
divergence that only shows up as a signer rejection. Now there is one.

The two `continue` guards immediately above it stay, and correctly so: a
foreign `destinationRollupId` and an empty `l2ToL1Calls` are *filters*
("not ours" / "nothing to deliver"), not errors. Only what survives the filters
gets shape-checked.

### Hunk 3a — `check_entry_shape` (the ex-closure, now `pub`)

Same four rejections in the same order (multi-call, nested, unsuccessful,
static/revert-span/explicit-gas), same messages, plus a doc comment that
carries over the original "would be SILENTLY TRUNCATED to call[0]" reasoning.
The added paragraph is the important one: the composer calls it per entry at
accept time and `build_cross_chain_sync_pairs` calls it over the whole set, so
both gate on the exact same predicate.

### Hunk 3b — `build_outbound_pair` (new)

The ex-PHASE-1 loop body, lifted verbatim into a per-entry function:
shape-check → take `l2ToL1Calls.first()` → `build_l2_outbound_entry` →
`build_outbound_load_table_txs(slice::from_ref(&entry), cfg, starting_nonce)`
→ wrap each load into a `SyncPair` with the user tx. (One load per entry
today — `build_outbound_load_table_txs` emits one `loadExecutionTable` per
entry — so this returns a single pair per call, but the `Vec` shape is
preserved so the nonce arithmetic stays honest if that ever changes.)

Why it exists: the drain appends pairs incrementally as each survivor is
accepted (`composer.rs:1884`), and the post-drain canonical rebuild calls the
same function over the same entries in the same order. So the composer's
incrementally-built block and the deriver/signer's canonical rebuild are
byte-identical **by construction** — which is what gives the drain's keystone
assert ("appended list == canonical rebuild") its teeth. If the drain had kept
its own copy of this lowering, that assert would compare two copies of the same
bug and pass.

### Hunk 4 — the pre-gate in `build_cross_chain_sync_pairs`

**Before:** two pre-gate loops — all outbound entries checked, then all inbound
— before anything was emitted.

**After:** only the inbound loop remains; outbound is gated inside PHASE 1 by
`build_outbound_pair`, which checks its entry before building anything.

The inbound half must stay, and the comment gives the real reason:
`build_inbound_system_txs` `continue`s past foreign-destination entries
*before* checking them, so without this loop an ill-shaped entry addressed to
another rollup would never be shape-checked at all. The pre-gate is
deliberately stricter than the emit path.

The one accepted behavior change: with **both** a bad outbound and a bad
inbound entry, the reported error flips from the outbound one to the inbound
one, since the inbound loop now runs first. Error text only — the call fails
either way, and nothing escapes (`pairs` is a local vec discarded on `Err`).

Small note on the comment "a bad shape must fail the call, not half-build it":
for outbound that was never a risk, since the function returns all-or-nothing.
The sentence is true of the inbound gate's *purpose* (check before emitting),
but the operative justification is the foreign-entry one in the next line.

### Hunk 5 — PHASE 1 body replaced by the call

```rust
let built = build_outbound_pair(entry, user_tx, cfg, nonce)?;
nonce = nonce.checked_add(built.len() as u64)…;
pairs.extend(built);
```

`built.len() == loads.len()` (one pair per load), so the nonce advance is
identical to before, overflow check included. Pure move.

## `crates/eez-protocol/src/abi.rs`

### Hunk 1 — four new `sol!` declarations

`authorizedProxies(address)`, `createCrossChainProxy(address,uint64)`,
`computeCrossChainProxyAddress(address,uint64)`, and
`executeOnBehalf(address,uint64,bytes)`.

Pure addition. The new L1 executor (`local/slot.rs`) replays the real
`_processNCalls` path rather than shortcutting it — look up the proxy, deploy
it permissionlessly if absent, then call through `executeOnBehalf` — so it
needs these four signatures. `abi.rs` is the workspace's single ABI source, so
they belong here rather than as file-local `sol!` macros next to each call
site.

I checked all four against the pinned submodule (`eez-core-protocol` @
`6fcc90b6`, matching the module's "ABI pins from commit 6fcc90b"):
`CrossChainProxy.sol:50` matches `executeOnBehalf` including `payable`;
`EEZBase.sol:156/176` match the two proxy functions; and the
`authorizedProxies` getter's flattened return `(bool, address, uint64)`
matches `struct ProxyInfo` in `IEEZ.sol:157`. The doc comment noting that
Solidity flattens the struct into its three members is a helpful line to keep.

### Hunk 2 — `manager_and_proxy_selectors_match_upstream`

Four selector asserts with an explicit drift message each. I recomputed them
with `cast sig` and all four match the pinned bytes: `0x8205f3e1`,
`0xa7587c62`, `0xeb20c0aa`, `0x360d95b6`.

This is the workspace's established guard, and the right one to reach for
here: bytecode-coupled constants have bitten this project before (the
`authorizedProxies` slot-constant episode), and a silently drifted selector on
the manager path would surface as an unexplained `EmptyCalls`-shaped failure
rather than a compile error.

## `crates/eez-composer/Cargo.toml` + `Cargo.lock`

One new dev-dependency: `reth-provider` with the `test-utils` feature, for
`MockEthProvider` in the prefix-state tests, plus the one-line lock entry. The
cost is real and was flagged consciously: `reth-provider` is already a
workspace dependency, so under resolver 2 the extra feature unifies across the
graph for any invocation that builds tests — `cargo test` and
`clippy --all-targets` pull a second feature set over the reth crates and pay
the compile time. Production builds (`cargo build -p eez-node`) are untouched,
since dev-dependency features never apply there. The alternative was no unit
coverage at all for the one equivalence the whole design rests on — every
other test in this workspace spawns real nodes — so this is the right call;
worth revisiting only if CI wall time becomes the constraint.

## Behavior-change inventory

| Change | Before | After |
|---|---|---|
| `BuiltSyncBlock.tx_successes` | built block carried no receipt statuses; a reverted tx in the final block was invisible to the caller | per-tx receipt status surfaced; composer gates dispatch on all-success and degrades to a minimal postBatch otherwise |
| `next_block_attributes` / `recover_tx` / `open_draft_db` extraction | attributes + decode/recover inline in `build_sync_block` | shared helpers; `build_sync_block` and `SyncBlockState::open` cannot disagree on the block env |
| `SyncBlockState` / `SyncBlockFork` / `TxOutcome` | none — no live mid-block state existed; composition simulated over `provider.latest()` | new public API: live prefix state over the block under construction, forks for probes/source sims, per-tx outcome including revert data |
| `build_sync_block` core path | builder → pre-exec → per-tx → `finish` | unchanged (same order, same env, same `finish`); only the receipt mapping is new |
| `sync_block_pair_roots` | rebuilds the block per pair-end for roots | none (untouched — roots require block close, which `SyncBlockState` deliberately never does) |
| `build_inbound_system_txs` shape rejection | third inline copy of the predicate | calls shared `check_entry_shape`; identical rule, identical messages; foreign/empty `continue` filters unchanged |
| `check_entry_shape` | private closure inside `build_cross_chain_sync_pairs` | public fn, same four rejections in the same order (pure lift) |
| `build_outbound_pair` | inline PHASE-1 loop body | public per-entry fn; the drain and the canonical rebuild now share one lowering, making the "appended == rebuilt" assert meaningful |
| Pre-gate ordering in `build_cross_chain_sync_pairs` | outbound entries gated first, then inbound, before any emission | inbound-only pre-gate (foreign entries would otherwise never be checked); outbound gated per entry inside PHASE 1. With one bad entry of each kind, the reported error flips outbound → inbound. Error text only |
| Nonce arithmetic in PHASE 1 | `nonce += loads.len()`, checked | `nonce += built.len()`, checked — same count (one load per entry) |
| Four `sol!` declarations + selector test in `abi.rs` | none (pure addition) | manager/proxy ABI lives in the single ABI source; all four selectors pinned and verified against `eez-core-protocol@6fcc90b` |
| `reth-provider` `test-utils` dev-dependency | none (pure addition) | test/clippy builds compile a second reth feature set; production builds unaffected |

### Follow-ups worth a line somewhere (neither blocking)

1. **Block-gas overflow is a whole-window failure.** The live path
   deliberately skips the builder's block-gas precheck, so a drain that
   overshoots `BUILDER_GAS_LIMIT` fails at the final `build_sync_block` and
   degrades + re-queues the same set, which will fail identically next slot. A
   cumulative-gas counter on `SyncBlockState` that evicts the offending tx would
   turn it into a per-tx eviction — the same shape as design §8 finding 2, one
   layer down.
2. **`SyncBlockState::fork` could take `&self`** — it only clones the cache, and
   the immutable signature would state "forking cannot disturb the block" in
   the type rather than in a comment.


---

# Part 2 — The slot execution contexts


Scope of this section, in reading order:

1. `crates/eez-composer/src/local/slot.rs` — new file, 747 lines, staged. Reviewed whole, block by block.
2. `crates/eez-composer/src/local/client.rs` — diff vs `HEAD`, hunk by hunk.
3. `crates/eez-composer/src/local/mod.rs` + `crates/eez-composer/src/lib.rs` — visibility/export diff.

The one sentence that frames everything below: **no approximations on the claim path.** The
old target session (`local/session.rs`, still in the tree, no longer used by the drain) computed
a claim by calling the target contract *directly*, with a computed proxy address forged into
`msg.sender`, every EVM check disabled, and a nonce-restore hack to undo the damage. The two
executors in `slot.rs` instead run the code the chain will actually run — `EEZ._processNCalls`'
frames on L1, the canonical `executeIncomingCrossChainCall` delivery tx on L2 — so the numbers
that end up in an entry's rolling hash are read off real executions rather than modelled.

---

## 1. `crates/eez-composer/src/local/slot.rs`

### 1.1 Module doc (`slot.rs:1-12`)

Three lines that pay for themselves: both types implement the same `TargetExecutionSession`
trait, "but neither approximates the protocol: each runs the real contract path the chain will
run", and each names the contract lines it mirrors (`EEZ.sol:1149-1178`, `EEZL2.sol:547-552`).
Those citations are load-bearing for a reader who wants to verify the port, and I checked them —
they land on the `sourceProxy.call{value:…}(abi.encodeCall(CrossChainProxy.executeOnBehalf, …))`
sites plus the `_rollingHashCallEnd(success, retData)` fold on both chains. Good.

### 1.2 Imports (`slot.rs:14-42`)

Mechanical. Worth noting only that the ABI surface (`authorizedProxiesCall`,
`computeCrossChainProxyAddressCall`, `createCrossChainProxyCall`, `executeOnBehalfCall`) comes
from `eez_protocol::abi` — the single ABI source, with selector-pin tests added in the same
change-set — not from ad-hoc `sol!` blocks here. And `DIRECT_CALL_GAS_LIMIT`, `evm_err`,
`provider_err` are reused from `session.rs`, so the old file stays as the home of those shared
bits even though its session type is now dormant on this path.

### 1.3 `LocalComposeClients` (`slot.rs:44-54`)

Two `Arc<LocalChainClient>`s, L1 entry and L2 entry, carried on `CrossChainWiring`
(`composer.rs:143`) next to the existing type-erased `ChainClient` map, and populated in
`eez-node/src/main.rs:697`.

**Why it exists.** The drain needs surfaces the `ChainClient` trait deliberately does not have:
`L1SlotState::open`, `simulate_source_tx_on`, `chain_provider()`. The doc comment says exactly this —
"both point at the same instances — the erased trait hides the simulation surfaces the drain
drives". That is the honest framing: this is not a second registry, it is a concrete-typed view
onto the same two clients.

**Why not the obvious alternative** (widen `ChainClient` with `fn open_world(&self)`): every
`ChainClient` impl would then have to answer a question only the local reth-backed one can —
the trait exists so that a future non-local client can be registered, and it would have to
stub-or-lie. This is the "stub that lies" anti-pattern, avoided by keeping the concrete handle
beside the erased one.

### 1.4 Constants and `encoding_err` (`slot.rs:56-66`)

`VIEW_CALL_GAS_LIMIT = 1_000_000` for the two view frames; `ZERO_CALL_GAS = 0` as
`executeOnBehalf`'s `callGas` argument, with the reason inline — zero means "forward all
remaining gas" (`CrossChainProxy.sol:60`) and is the only shape the protocol emits since
`USE_GAS_LEFT` is off everywhere.

`encoding_err` looks like a formatting helper but it is a **policy** helper, and that deserves a
sentence in review. The drain classifies failures by `ExecutorErrorKind`
(`composer.rs:409-426`): `Unavailable`/`Provider`/`Missing` are TRANSIENT (re-queue the tx,
abort the slot), everything else is POISON (evict this tx, keep composing). So the kind chosen
at each failure site *is* the eviction decision:

- `L1SlotState::open` uses `provider_err` / `Missing` → a provider hiccup at anchor time re-queues,
  it does not evict user transactions. Correct.
- every failure caused by the transaction itself — manager frame reverted, target reverted,
  probe never reached the proxy frame, delivery reverted — goes through `encoding_err` → poison.
  Correct policy, slightly misleading name; `Encoding` reads like "ABI problem" when it now
  also means "this tx is structurally undeliverable". Not worth churn now, but if a new kind is
  ever added, `ExecutorErrorKind::Rejected` would document itself.

### 1.5 `L1SlotState` (`slot.rs:68-157`)

```rust
pub struct L1SlotState {
    pub anchor: SealedHeader<Header>,
    pub cache: CacheState,
}
```

**One per drain**, created at `composer.rs:1661`. The anchor is the L1 head at drain start and
never moves. That pin is the point: the bundle lands at least one L1 block later no matter what,
so a "fresher" base buys nothing real, and a *moving* base would make a transaction's claims
depend on when it happened to arrive relative to L1 block production — the same set of held txs
would compose differently on two runs. Pinning makes the drain a pure function of (held set,
anchor). Design §5 owns the residual approximation ("L1 base drift") and lists the on-chain
containment: `StateUpdate.currentState` gates at consumption, immediates skip rather than abort,
deferred consumption is prefix-partial, and the optimistic observer reorgs L2 on any
less-than-claimed settlement.

`cache` is the commit-or-drop ledger. It holds **only** effects of transactions that survived —
the drain writes it at accept points and nowhere else. That is why eviction needs no unwind
machinery: a poisoned tx's fork is simply dropped (design §4.7, "rollback is structural, not
mechanical").

- `open` (`slot.rs:92-110`) — best block number → header → `seal_slow`, empty cache, one debug
  line naming the pinned block/hash. Errors are `Provider`/`Missing`, i.e. transient. Fine.
- `open_state` (`slot.rs:114-137`) — the single door every fork goes through: state at the
  anchor **hash** (not "latest", so it cannot drift mid-drain), the anchor's EVM env, and
  `with_cached_prestate(seed)` to preload accumulated effects. `with_bundle_update()` is on but
  nothing reads the bundle; see the checkpoint note in §1.7.
- `fork_state` (`slot.rs:148-156`) — the inbound source-sim fork: anchor state + world cache +
  the **plain** anchor env. The comment explains the split precisely: `simulate_source_tx_on`
  applies its own source-sim cfg tweak (nonce check off), and the manager-frame tweaks
  (base fee / EIP-3607 / gas cap) deliberately stay out of a path that executes a *real signed
  user transaction*. Keeping those two envs distinct is the sort of thing that is easy to
  collapse "for tidiness" and would quietly weaken the inbound sim.

**Observation (not a bug).** `open_state` builds the env with `evm_env(self.anchor.header())` —
the env *of* the anchor block, not `next_evm_env` for anchor+1. So a target reading
`block.number` / `block.timestamp` inside a manager frame sees the anchor's values while the
real execution will see anchor+1 or later, and `frame_gas` clamps to the anchor's gas limit
rather than the landing block's. This is the same bounded L1 base drift §5 already accepts, and
using `next_evm_env` would be a *different* guess, not a truer one. Worth one comment line at
`open_state` so the next reader doesn't have to derive it.

### 1.6 `L1TargetSession` — struct, `Debug`, `new` (`slot.rs:161-220`)

This is the outbound target session: for an L2→L1 call it replays exactly what
`EEZ._processNCalls` will do inside the future `postAndVerifyBatch`.

`new` opens a fork of the world (`world.cache.clone()` seeded) and then makes exactly four env
edits:

```rust
evm_env.cfg_env.disable_base_fee = true;
evm_env.cfg_env.disable_eip3607 = true;
evm_env.cfg_env.disable_nonce_check = true;
evm_env.cfg_env.tx_gas_limit_cap = Some(u64::MAX);
```

**Before → After that matters here.** The old path called `session::disable_checks`
(`session.rs:398-402`), which additionally set `disable_balance_check = true` and
`disable_block_gas_limit = true`. Both are gone, on purpose, and the comment says why for the
first: the frames are synthetic (no fee market, no EOA sender) so base-fee/3607/nonce checks are
noise, **but the balance check stays on so escrow is real**.

Concretely: an L2→L1 withdrawal of 1 ETH. Old world — `msg.sender` was a forged proxy address
with balance checks off, so the value was conjured and the sim always "succeeded". New world —
the value is drawn from the manager's actual balance at the anchor, exactly as
`sourceProxy.call{value: …}` will draw it on-chain; if the escrow is short, the sim fails here,
at compose time, costing one eviction, instead of at the builder's bundle simulation or after
settlement.

`Debug` is hand-written to print `manager`/`chain_id` only — the revm `State` isn't `Debug` and
would be unreadable anyway. Mechanical.

### 1.7 `frame_gas` (`slot.rs:225-227`)

```rust
fn frame_gas(&self, requested: u64) -> u64 {
    requested.min(self.evm_env.block_env.gas_limit)
}
```

Three lines with a live-chain story behind them (design §8, finding 1). This clamp was added
last, after chiado testing. chiado's L1 block gas limit is ~17M; `DIRECT_CALL_GAS_LIMIT` is 30M;
revm refuses any transaction whose gas limit exceeds the block's, so **every** manager frame
failed with "caller gas limit exceeds the block gas limit" — and since that failure is poison,
every outbound composition evicted. Dev chains mask it completely (block limit ≥ 30M), which is
why it only surfaced on a real chain.

**Why not the obvious alternative** — `disable_block_gas_limit = true`, which is what the old
session did? Because clamping is *truer to the chain*: on-chain a call genuinely cannot be given
more gas than the block allows, so a 25M-gas target that "passes" under a disabled limit would
be a claim the L1 execution can never honor. Clamping fails the same transactions L1 would fail.
The one-line doc comment on the fn says this in the same number of words.

### 1.8 `manager_frame`, `view_call`, `proxy_address`, `is_authorized_proxy`, `create_proxy` (`slot.rs:231-313`)

`manager_frame` is the shared primitive: a synthetic tx from `Address::ZERO` to the manager with
supplied calldata and clamped gas, run on the fork; a revert becomes a poison error carrying the
raw output; on the `commit` path the caller's nonce bump is undone (see §1.10) before
`self.state.commit(changes)`.

The three callers mirror `EEZ.sol:1149-1152` one-for-one and the doc comments cite those lines:

| Rust | Solidity |
|---|---|
| `proxy_address` | `computeCrossChainProxyAddress(l2ToL1Call.sourceAddress, l2ToL1Call.sourceRollupId)` |
| `is_authorized_proxy` | `if (!authorizedProxies[sourceProxy].isProxy)` |
| `create_proxy` | `_createCrossChainProxyInternal(...)` via the permissionless `createCrossChainProxy` |

Two details worth calling out because they are the difference between "mirrors the contract" and
"looks like it mirrors the contract":

- The proxy address is obtained by **asking the manager**, not by recomputing CREATE2 in Rust.
  Memory `project_eez_iter9_10_partial` is exactly the scar tissue here (ported slot constants
  that disagreed with our contracts). A view frame cannot drift from the deployed bytecode.
- `create_proxy` runs the **real** `createCrossChainProxy`, so the CREATE2 deployment happens
  from the manager's own frame with the manager's salt and the manager's `CrossChainProxy`
  creation code (`EEZBase.sol:160-171`), and the `authorizedProxies[proxy] = ProxyInfo(...)`
  registration lands in the fork. Deploying a proxy "by hand" into the cache would be a
  bytecode-coupled guess of exactly the kind that broke before.

Note `create_proxy` uses `DIRECT_CALL_GAS_LIMIT` (clamped) rather than `VIEW_CALL_GAS_LIMIT`: a
CREATE2 deployment is not a view. Correct.

### 1.9 `impl TargetExecutionSession for L1TargetSession` (`slot.rs:316-412`)

**`execute` (`slot.rs:317-397`)** — the heart of the outbound path, in the contract's own order:

1. `CallMode::Static` refused (`Unavailable` → transient, same as the old session; static entries
   are parked, design §6).
2. compute proxy → if unauthorized, create it.
3. build the frame: `caller = manager`, `to = proxy`,
   `data = executeOnBehalf(target, 0, data)`, `value = req.value`.

That shape is the whole point. The proxy's `executeOnBehalf` is transparent-proxy-style: it
forwards only when `msg.sender == EEZ` and otherwise falls through to the cross-chain path
(`CrossChainProxy.sol:50-64`). So the *only* way to get a faithful target execution is to enter
through the manager. The old direct call with a forged proxy `msg.sender` bypassed the proxy
contract entirely — a target that inspects `msg.sender` (or the proxy's own accounting) saw a
different world than the chain will show it.

4. The frame runs under `SkipTopFrame::new(self.client.inspector_factory().build(dispatcher))`
   — see §1.10.
5. `restore_nonce(&mut changes, self.manager)` then `commit`.
6. The outcome is returned **raw**:

```rust
Ok(ExecutionOutcome::Resolved { return_data: return_data.to_vec(), gas_used, success })
```

with the comment "The frame's raw output IS what `_processNCalls` folds into CALL_END
(`EEZ.sol:1181`) — revert data included on failure." Verified against the contract: it does
`(success, retData) = sourceProxy.call{value: …}(…)` and then `_rollingHashCallEnd(success,
retData)`. And because `executeOnBehalf` re-reverts with the target's own revert bytes
(`revert(add(result,0x20), mload(result))`), a failing target yields byte-identical revert data
here and on-chain. No post-processing, no normalization — anything else would be a divergence
source.

One thing that looks wrong on a fast read and is right: `commit` happens even when
`success == false`. revm has already discarded the reverted frame's journal, so `result.state`
for a failed tx carries only touched accounts and the caller's nonce/fee changes — and the nonce
is restored and the gas price is zero. Meanwhile the proxy *creation* from step 2 stays
committed, which is exactly the on-chain behavior: `_processNCalls` creates the proxy first and
catches the call failure as `(false, retData)`, so the deployment survives the failed call.

**`checkpoint` / `rollback` (`slot.rs:399-411`)** — the payload contract, documented on the impl
(`slot.rs:163-170`) and consumed at `composer.rs:589-605`:

```rust
fn checkpoint(&mut self) -> ExecutorResult<SessionSnapshot> {
    Ok(Box::new(self.state.cache.clone()))
}
```

`SessionSnapshot` is `Box<dyn Any + Send>`, so the type is checked at runtime only. The drain
reclaims the boxed session via `take_sessions`, calls `checkpoint()`, downcasts to `CacheState`
and commits it into `L1SlotState::cache` on accept — `take_l1_cache` even repeats the constraint
in its own doc ("The payload shape is pinned by `L1TargetSession::checkpoint`: a boxed `CacheState`
and nothing else"). Documenting it on both ends is right given `Any` gives no compiler help.

The double duty is neat rather than clever: the same method serves (a) the builder's intra-tx
revert-span rollback and (b) the drain's end-of-tx harvest, because "the accumulated effects" is
the same object in both cases.

Cache-only restore is sound because simulation reads go exclusively through the cache; the
bundle/transition state is never consulted. The comment says that. **Before → After**: the old
session's `checkpoint` returned `Box::new(())` and its `rollback` was a no-op type check
(`session.rs:345-362`), with a comment admitting that annulled-call safety rested entirely on
batch materialization rejecting revert spans. So a reverted span inside a composition left its
writes in the session. Now a revert span actually rewinds.

### 1.10 `SkipTopFrame` (`slot.rs:414-450`)

Found in author review of the first implementation, and it is the subtlest thing in the file.

The session inspector fires on **every** call frame, and its job is: if the callee is an
authorized proxy, intercept the frame and re-dispatch it through the composition builder. But the
manager frame's own callee *is* an authorized proxy — so without the wrapper the inspector would
intercept the very frame that IS the dispatch and re-dispatch it as a nested call. In practice
every outbound transaction would have poison-evicted.

The wrapper is 30 lines and hides exactly one frame:

```rust
fn call(&mut self, context: &mut CTX, inputs: &mut CallInputs) -> Option<CallOutcome> {
    let top = self.depth == 0;
    self.depth += 1;
    if top { return None; }
    self.inner.call(context, inputs)
}
```

Nested proxy calls made *inside* the target still forward to the inner inspector and are recorded
as `expectedL1ToL2Calls`, which the accept-time shape gate turns into a precise per-tx eviction
(nested composition is parked, design §6). So the wrapper narrows the blindness to one frame
rather than turning inspection off.

The documented consequence — "a revert of the top frame itself is not span-annotated" — is exact:
`SessionInspector::call` pushes a dispatch-count marker and `call_end` pops it to emit
`annotate_revert_span` (`eez-evm-inspector/src/inspector.rs:242-247, 361-375`). Skipping both
halves for the top frame means no span annotation for it, which is acceptable precisely because
any composition with nested dispatches is shape-gated to eviction anyway.

Three things I checked rather than assumed:

- **Stack balance under interception.** If the inner inspector returns `Some(outcome)` from
  `call`, does revm still deliver `call_end`? Yes — `revm-inspector 19.0.0`
  (`traits.rs:104-107`) calls `frame_end` immediately after a short-circuiting `frame_start`. So
  both `SkipTopFrame::depth` and `SessionInspector::frame_starts` stay balanced. If that ever
  changed upstream, both counters would skew; the depth field is the kind of thing worth a
  regression test if the revm pin moves.
- **Skipping both halves, not one.** Forwarding `call_end` while skipping `call` would
  underflow the inner inspector's `frame_starts` and mis-attribute the next frame's revert span.
  The `saturating_sub` + `> 0` check pairs them correctly.
- **Hook coverage.** `SkipTopFrame` overrides only `call`/`call_end`; every other `Inspector`
  hook falls back to the trait default (no-op) and therefore does **not** reach `inner`.
  `SessionInspector` today implements exactly `call` and `call_end`, so nothing is dropped. Worth
  a one-line comment on the wrapper, because the day someone adds `create_end` or `log` to
  `SessionInspector` it will be silently ignored on this path only.

### 1.11 `restore_nonce` (`slot.rs:452-465`)

revm bumps a transaction sender's nonce. Here the "senders" are the manager contract and the
synthetic `Address::ZERO` proxy-creation caller — neither is an EOA, and a contract account's
nonce only governs CREATE, while every proxy is CREATE2. So the bump would drift the fork from
the real chain for no reason, and the function resets `info.nonce` to `original_info.nonce`.

**Before → After.** The old `restore_caller_nonce` (`session.rs:386-397`) did the same
mechanical thing for a much worse reason: the old session forged `msg.sender` as the *computed
proxy address*, and bumping that address's nonce made a later real `CREATE2` at the same address
fail EIP-684's "code or nonce non-zero" check, burning ~28M gas and reverting the whole session
with empty data. The 12-line comment documenting that failure mode is a good marker of how much
the shortcut cost. In the new design the shortcut is gone, so the remaining nonce restore is
plain hygiene on synthetic senders — and the new comment is three lines instead of twelve,
correctly.

### 1.12 `ProbeSnapshot` (`slot.rs:469-474`)

Cache **plus** `delivery_nonce`. Both halves must rewind together: if a revert span rolls back a
delivery, the SYSTEM_ADDRESS nonce cursor must go back with it or every subsequent delivery in
the composition is built at the wrong nonce and fails at execution. Small struct, real
invariant.

### 1.13 `InboundL2TargetSession` — struct, `new`, `l1_entry_for_call`, `delivery_tx` (`slot.rs:476-568`)

The inbound target session. Its state is a `SyncBlockFork` (a throwaway copy of the Sync block
under construction, `build.rs:395-471`), the `SystemTxContext`, and the delivery nonce cursor.

`l1_entry_for_call` (`slot.rs:517-553`) builds the **L1-shape** `ExecutionEntrySol` for one inbound call
— the same shape a postBatch carries — because `build_inbound_system_txs` is the canonical
batch→delivery lowering used by the deriver and the signer. Reusing it means the probe's delivery
transaction is byte-identical to what a follower will reconstruct from L1, by construction rather
than by review. That is the same "single-source STF" discipline the composer/deriver split
already relies on.

`delivery_tx` (`slot.rs:556-567`) lowers exactly one entry at the current cursor and asserts the
one-in/one-out shape:

```rust
let [tx] = <[Bytes; 1]>::try_from(txs).map_err(|txs| encoding_err(format!(
    "one inbound entry must lower to exactly one delivery tx; got {}", txs.len())))?;
```

That is not paranoia: `build_inbound_system_txs` silently *skips* entries whose
`destinationRollupId` doesn't match `cfg.this_rollup_id` (`system_tx.rs:89-91`), so a rollup-id
mix-up would otherwise yield an empty vec and a confusing downstream failure. Loud, per
invariant 7.

**Small observation.** `l1_entry_for_call` computes the lean L2 entry to obtain `proxyEntryHash` and
`rollingHash`, but `build_inbound_system_txs` rebuilds that same lean entry internally from the
L1-shape fields (`system_tx.rs:103-113`), so those two fields on the returned struct are inert
for the lowering. They are identical by construction (same builder, same inputs), so this is
correctness-neutral — but a reader can spend a while looking for where the hashes are consumed.
One clause on the doc comment ("the hashes are recomputed by the canonical lowering; set here so
the entry is well-formed") would save that trip.

### 1.14 `impl TargetExecutionSession for InboundL2TargetSession` (`slot.rs:570-678`)

**Why two runs.** This is inherent, not an optimization choice, and the code says so. The rolling
hash must be computed *inside* the transaction that produces it — `EEZL2._executeEntry` seeds it
from `proxyEntryHash`, folds `(success, retData)` at `EEZL2.sol:551`, and compares against the
claimed `entry.rollingHash` at `EEZL2.sol:466`. You cannot learn the true return data and land
the final transaction in one pass, because the final transaction's own input depends on its own
output.

**Run 1, PROBE (`slot.rs:583-625`).** The entry is built with the *correct* `proxyEntryHash` —
the call hash is computable a priori, it folds identity (`isStatic`, source, source rollup,
target, target rollup, value, `callGas = 0`, data) but not return data — and a placeholder
`returnData`. That matters: with the right entry hash the delivery passes
`if (entry.proxyEntryHash != crossChainCallHash) revert EntryHashMismatch;`
(`EEZL2.sol:308-311`) and reaches the real `EEZL2 → proxy → target` call. The transaction is
then **expected** to revert at `RollingHashMismatch` — by which point the frame has already run
and `ProbeInspector` has captured its `(success, retData)`, which is precisely what
`_processNCalls` folds at `EEZL2.sol:547-552`.

The snapshot/restore around the probe is load-bearing, not hygiene:

```rust
let snapshot = self.fork.cache_snapshot();
… execute probe …
self.fork.restore_cache(snapshot);
```

A reverted transaction still burns the SYSTEM_ADDRESS nonce and gas. Leaving the probe's effects
in place would make the real run at the same nonce fail — the mechanism would eat itself. The
inline comment frames it as "the probe leaves no trace: its state effects are re-applied by the
real run below", which is true but understates the necessity; "the probe must not consume the
nonce the real run needs" is the sharper reason.

The three-way match on `inspector.captures` is good failure design: zero captures means the
delivery never reached the proxy frame (entry hash or table mismatch) and the message carries the
probe's own success/output for diagnosis; more than one means a nested or multi-call shape, which
is parked, and says so. Both are poison-kind, i.e. one eviction, not a slot degrade.

`!captured.success` → poison (`slot.rs:619-625`). Current policy: a *reverting target* on the
inbound path cannot be represented, because `build_l2_incoming_entry` rejects `success == false`
(`entries:279-281`). Worth being explicit that this is a policy statement about today's entry
builders, not a protocol limit — the contract has a reverting-entry path
(`EEZL2.sol:471-478`).

**Run 2, REAL (`slot.rs:627-647`).** Rebuild the entry with the captured output — the canonical
builder recomputes the rolling hash with the shared fold — lower it, and execute on the fork. It
**must** succeed:

```rust
if !real.success { return Err(encoding_err(format!(
    "canonical delivery for {} reverted on the block prefix at SYSTEM nonce {}: {}", …)));
}
```

This is the line that closes issue #88. It is the same on-chain claim verifier that previously
fired at the proof signer — where it froze the whole window and the drain re-queued the same set
to fail identically next slot — now firing at compose time, where the cost is one eviction. The
verification moved, it did not get weaker.

**Before → After, concretely (the #88 repro, design §9.1).** Three `increment()` calls against
one stateful L2 target in one drain. Before: each was simulated against the same pre-slot state,
so all three claimed a return of `1`; on-chain they execute sequentially, produce `1, 2, 3`, and
the second delivery reverts `RollingHashMismatch` → the signer rejects the window → re-queue →
identical failure forever. After: each probe runs on the fork of the block prefix that already
contains the previous accepted delivery, so the claims are `1, 2, 3` and the window signs.

Nonce advance is `checked_add` with a loud error (`slot.rs:645-647`), and `checkpoint`/`rollback`
carry both halves of the state as described in §1.12.

**Small nit.** `SyncBlockFork::execute_tx*` increments its internal `applied` counter, and
`restore_cache` does not rewind it, so after a probe the fork's counter is one ahead of the
transactions actually applied to it. `applied` is only passed as `idx` to `recover_tx` for error
messages (`build.rs:248-270`), so this is cosmetic today — but if it ever feeds a block position
or a receipt index it becomes real. Either add `applied` to `ProbeSnapshot` or note on
`restore_cache` that the counter is diagnostic only.

### 1.15 `ProbeCapture` / `ProbeInspector` (`slot.rs:680-747`)

`ProbeCapture` is `(success, output)` — deliberately no gas field (the earlier draft had
`gas_used`; it was removed in audit, and rightly: `USE_GAS_LEFT` is off and hashes fold
`callGas = 0`, so gas is not consensus-relevant on this path and an unused field invites someone
to start trusting it).

The match predicate is the contract's own call shape:

```rust
let matched = !inputs.is_static
    && matches!(inputs.scheme, CallScheme::Call)
    && inputs.caller == self.eezl2_address
    && inputs.input.as_bytes(context).starts_with(&executeOnBehalfCall::SELECTOR);
```

i.e. exactly the non-static `sourceProxy.call{value:…}(abi.encodeCall(executeOnBehalf, …))` at
`EEZL2.sol:547-550`. The `caller == EEZL2` clause is what keeps a target that happens to call some
other proxy from being mistaken for the delivery frame.

Capture happens in `call_end`, not `call` — the outcome is only known at frame end — with a
`frames: Vec<bool>` acting as a depth/match stack so nested frames pop their own marker. Since
`call` always returns `None` (this inspector observes, never intercepts) the pairing is
unconditional.

Post-audit it is a plain `Vec` read through a `&mut` borrow (revm auto-implements `Inspector` for
`&mut I`, so `execute_tx_inspected(&probe_tx, &mut inspector)` type-checks and the caller keeps
ownership). That replaced an `Arc<Mutex<…>>`: same guarantees, no lock, no poison branch, and the
capture list is readable straight after the run. Right call — the inspector and its reader are
the same thread, always.

`ProbeInspector::new` takes the EEZL2 address rather than reading it from the context; the hand
written `Debug` prints the address and the open-frame depth. Mechanical.

### 1.16 Absent relative to the earlier draft

Noted so a reviewer who saw the first version doesn't go looking: `final_cache`, `into_fork`, the
`delivery_nonce()` accessor, two vacuous tests, and `ProbeCapture.gas_used` were all removed in
audit — dead API surface that no caller reached. There is no `#[cfg(test)]` block in the file;
coverage lives in `crates/eez-node/tests/chained_interstate.rs` plus the chiado runs of design §8,
which is the right level for something whose whole contract is "the real chain agrees".

---

## 2. `crates/eez-composer/src/local/client.rs` (diff vs `HEAD`)

The theme: one source-simulation body, two entry points, and everything `slot.rs` needs exposed
as small accessors.

### Hunk 1 — imports (`client.rs:16-17`)

`StateProviderBox` for the caller-provided state type in the new signature, `revm::DatabaseCommit`
for the new commit. Mechanical.

### Hunk 2 — three accessors (`client.rs:167-190`)

`chain_provider()`, `manager_address()`, `inspector_factory()` — all pure getters over data the
client already held, added because `slot.rs` needs them (`L1SlotState::open` reads headers/state,
`L1TargetSession::new` reads the manager address and the EVM config, `execute` builds the session
inspector).

`inspector_factory()` also removes a duplicate: `begin_execution_session` and the source-sim path
each hand-built the same three-argument `SessionInspectorFactory::new(...)`. Now both call the
accessor, so the proxy-lookup config / rollup id / overlay channel triple has one definition.

Audit deleted a fourth accessor, `rollup_id()` — no callers. Correct instinct: an accessor with no
consumer is API surface that has to be maintained and will eventually be used for the wrong thing.

### Hunk 3 — `simulate_source_tx_on` (`client.rs:192-211`)

The new public entry point. Same semantics as the trait method except the caller supplies the
state and env, and **the result commits into that state**.

That commit is what makes the source side chained. Concretely, two inbound L1 transactions from
the same sender at nonces 0 and 1 in one drain: with the old "open latest, discard" behavior,
tx 1's simulation would run against a state where tx 0 never happened — stale balance, stale
target storage, stale nonce. With the fork carried across, tx 1 sees tx 0's writes, so the call
arguments and return data it claims match the order the bundle will actually execute in. The same
mechanism carries phase-1 outbound manager effects into phase-2 inbound sims, since both draw
from `L1SlotState::cache`.

Nonce *validation* is a separate matter: `source_sim` sets `disable_nonce_check = true` (inherited
behavior, needed because a system-signed source tx can legitimately sit at N+1 behind its
`loadExecutionTable`), so the chaining benefit here is the visible **state**, including the
sender's nonce as a target or a CREATE would observe it — not nonce validation.

The doc comment points at the constraint that the caller owns: the env "must already be derived
from the fork's own header". `L1SlotState::fork_state` and `SyncBlockFork::state_and_env` are the two
places that hold up that end.

### Hunk 4 — `source_sim`, the shared body (`client.rs:213-283`)

Everything from the entry-role gate through decode, tx env, inspected run, inspector-error check
and commit now lives here once. Three things inside it are worth a line each:

- The entry-role gate stays in the shared body with its original comment ("callers use the uniform
  `ChainClient` interface for both roles") — so `simulate_source_tx_on` inherits the same refusal
  and a follower client cannot be smuggled onto the new path.
- The result is destructured to `(gas_used, success, changes)` with `changes: Option<_>`; a
  transact error logs `source sim reverted` and yields `None`, so nothing is committed on that
  branch. Same tolerant behavior as before for the trait path, and correct for the fork path.
- The commit is guarded by a comment explaining why it is unconditional: *"The trait-method
  caller's `State` is function-local, so committing into it is unobservable there; the fork caller
  needs the writes."* This is the answer to the obvious alternative — an earlier draft had a
  `CommitPostState` enum threading "commit or not" through the body, and audit removed it because
  the flag could only ever have one observable value. One less parameter, one less thing to get
  wrong at a call site; the comment carries the reasoning that the enum used to carry.

The timing instrumentation (`t_total`/`t_decode`/`t_state`/`t_env`/`t_sim` and the
`source simulation timing` event) is gone. Worth noting *why* rather than as a deletion: after the
split, `state_us` and `env_us` measured work the function no longer does — on the
`simulate_source_tx_on` path the state and env are handed in already open — so the fields would
have reported near-zero and lied about where time goes. Dropping a metric that has become
structurally wrong is better than keeping a familiar-looking number.

### Hunk 5 — `begin_execution_session` (`client.rs:291-313`)

Body unchanged except `Some(self.inspector_factory())` in place of the inline three-line
construction. Mechanical, and it moved below the inherent-impl block in the file — the diff looks
larger than the change because of that relocation.

### Hunk 6 — `simulate_source_tx` trait impl (`client.rs:315-350`)

Now just the "open at the tip" preamble — best block number → header → `provider.latest()` →
`State::builder()` → `evm_env(&header)` — followed by `self.source_sim(...)`. Behavior is
preserved bit-for-bit: it still opens latest, and its post-state still dies with the call, which
the closing comment states outright ("Inspect only: the post-state dies with this call"). This
matters because the trait method is still the path other (non-slot) callers use; the change-set
does not quietly alter it.

---

## 3. `crates/eez-composer/src/local/mod.rs` and `crates/eez-composer/src/lib.rs`

Small and worth being deliberate about.

`mod.rs`:

- module doc gains one line for `slot` — "slot-scoped execution contexts driving the real contract
  paths" — sitting alongside the existing `LocalChainClient` / `LocalExecutionSession` entries.
- `pub(crate) mod slot;` matches the visibility of `build`, `client`, `provider`, `session`
  (`gnosis_adapter` remains `pub`). Modules stay crate-private; only chosen items are re-exported.
- `pub use slot::LocalComposeClients;` (`#[doc(inline)]`) — the single type that must cross the crate
  boundary, because `eez-node/src/main.rs:697` constructs it when wiring `CrossChainWiring`.
- `pub(crate) use slot::{InboundL2TargetSession, L1SlotState, L1TargetSession};` with the comment "Slot
  execution contexts are driven only by `composer.rs`."

That last line is the interesting choice. The three executors are `pub` **within their module**
(they need doc comments, `#[must_use]`, and public methods for `composer.rs`), but the re-export
is `pub(crate)`, so the crate's external surface gains exactly one name. The alternative —
exporting all four — would publish types whose contracts are only meaningful inside the drain
(the `checkpoint`-payload hand-off, the anchor pinning, the accept-time commit protocol). Keeping
them crate-internal keeps those invariants enforceable by reading one file.

`lib.rs`: `LocalComposeClients` joins the existing `#[doc(inline)] pub use local::{…}` list. One name
added, nothing else touched.

---

## 4. Behavior-change inventory

| Change | Before | After |
|---|---|---|
| L1 target execution path | direct call to the target with a computed proxy address forged as `msg.sender` (`session.rs` `build_tx_env`) | real frames: manager → `proxy.executeOnBehalf(target, 0, data)` → target, mirroring `EEZ._processNCalls` (`slot.rs:329-350`) |
| Proxy existence | assumed; address computed, never deployed | `authorizedProxies` checked via the manager's own getter; missing → real permissionless `createCrossChainProxy` CREATE2 from the manager's frame (`slot.rs:290-313`) |
| Escrow / value | `disable_balance_check = true` — value conjured, sim always paid | balance check ON; value drawn from the manager's real balance, short escrow fails at compose time (`slot.rs:203-208`) |
| Frame gas | `disable_block_gas_limit = true`, 30M frames | clamped to the anchor block's gas limit (`frame_gas`, `slot.rs:225-227`); fixes the chiado 17M-limit poison-evict of every outbound tx |
| L1 state lifetime | fresh `provider.latest()` per composition, post-state dropped at `finalize` | one `L1SlotState` pinned at drain start, advanced commit-or-drop per surviving tx (`slot.rs:78-157`) |
| Target-session checkpoint | `Box::new(())`, rollback a no-op type check | real `CacheState` clone/restore; doubles as the drain's accept-time harvest payload (`slot.rs:399-411`) |
| Inbound claim resolution | direct call to the target produced the claim; the delivery tx was verified only on-chain / at the proof signer | probe the canonical delivery on a fork of the Sync block, capture the real `EEZL2 → proxy` outcome, then run the canonical delivery for real — must succeed (`slot.rs:583-647`) |
| Inbound nonce cursor | n/a | `delivery_nonce` advances per accepted delivery and rewinds with rollback via `ProbeSnapshot` (`slot.rs:469-474`, `663-677`) |
| Reverting inbound target | claim recorded, failure surfaced later on-chain | poison at compose time, one eviction, drain continues (`slot.rs:619-625`) |
| Top-frame inspection | n/a (no manager frame existed) | `SkipTopFrame` hides the outermost frame so the session inspector cannot re-dispatch the dispatch; nested proxy calls still recorded → shape-gated eviction (`slot.rs:414-450`) |
| Nonce restore rationale | undo a bump on the forged proxy `msg.sender` to keep the CREATE2 slot fresh (EIP-684) | undo a bump on synthetic/contract senders only; the forged-sender hazard no longer exists (`slot.rs:452-465`) |
| `simulate_source_tx` (trait) | opens latest, discards post-state | unchanged behavior; body now delegates to shared `source_sim` (`client.rs:315-350`) |
| Source sim over a caller fork | did not exist | `simulate_source_tx_on` runs over caller-provided state + env and commits into it, so tx N+1 sees tx N (`client.rs:192-211`) |
| Source-sim timing telemetry | five `timing.*_us` fields per call | removed — the field names described work the split function no longer does |
| Client accessors | none | `chain_provider()`, `manager_address()`, `inspector_factory()`; `rollup_id()` proposed and audit-deleted (no callers) |
| Crate surface | `local::{BuildError, BuiltSyncBlock, GnosisL1Adapter, LocalChainClient, build_sync_block}` | plus `LocalComposeClients`; the three executors stay `pub(crate)` |


---

# Part 3 — The drain rework


The drain used to simulate every held cross-chain tx in isolation against the
same pre-slot state. Co-bundled order-dependent txs therefore recorded claims
(returnData, and through it the rolling hash, and through *that* the entry
identity) that real sequential execution contradicts. Concretely: three
`increment()` calls drained into one slot all recorded `returnData = 1`; the
chain executes them as `1, 2, 3`; delivery #2 reverts `RollingHashMismatch`
on-chain; the proof signer refuses the window; the drain re-queues the same set
at the FIFO front and the next slot fails identically — a freeze (issue #88).

This file's half of the fix: the drain now keeps two slot-scoped worlds (the L1
world pinned at the anchor, and the Sync block under construction), composes
each tx on a *fork* of both, and — only on accept — appends the canonical txs to
the block and commits the L1 effects to the world. The next composition sees its
predecessors exactly as sequential execution will. Architecture:
`docs/CHAINED-INTERSTATE-DESIGN.md`.

Walkthrough in file order.

---

## Hunk 1 (~L43) — imports

Pulls in the new slot machinery: `L1SlotState`, `L1TargetSession`, `InboundL2TargetSession`,
`build::SyncBlockState`. Mechanical.

## Hunk 2 (~L125) — `CrossChainWiring`: erased clients out, concrete handles in

`entry_client` and `l2_entry_client` (both `Arc<dyn ChainClient>`) are replaced
by one field:

```rust
pub local: crate::local::LocalComposeClients,   // { l1_entry, l2_entry: Arc<LocalChainClient> }
```

**Why:** the drain now needs surfaces the erased `ChainClient` trait does not
expose — `L1SlotState::open`, `simulate_source_tx_on(.., state, env)`. Same
instances that are registered in `rollups`; they share one overlay channel, so
this is a second *view*, not a second client. It is a required field, not an
`Option`: exactly one construction site exists
(`eez-node/src/main.rs:697`) and it always has both.

## Hunk 3 (~L137) — `simulate_and_resolve` / `simulate_and_resolve_recorded_for` deleted, `compose_chained` added

Both `CrossChainWiring` methods are gone, replaced by a free function
`compose_chained(cc, entry_rollup_id, entry_client, raw_tx, sessions,
source_state, source_env)`. Same pipeline (reset overlays → build the `Rollup`
map → source-sim through the `CompositionBuilder` → `finalize`), three
differences:

1. **Sessions are pre-seeded by the caller**, via the existing
   `CompositionBuilder::with_sessions`. Tx N's target-side execution therefore
   runs on a context that already contains tx N-1's accepted effects. Before,
   every dispatch lazily opened a session at `provider.latest()`.
2. **Source simulation runs on a caller-provided fork state + env**
   (`simulate_source_tx_on`) instead of `provider.latest()`. So the *inputs* to
   the claim are sequential too — a state-dependent call *argument* is as
   fatal as a state-dependent return value (it changes the entry hash), which
   is why both halves had to move.
3. **Sessions are taken back before `finalize` and returned to the caller**:

   ```rust
   let sessions = builder.take_sessions();   // before finalize consumes the builder
   ```

   This is the commit-or-drop seam. The accepted effects live in the sessions,
   not in the `Composition`, so the caller commits them on accept and simply
   *drops* them on eviction. No rollback machinery is needed because a
   non-survivor never touched shared state.

New loud check, between `take_sessions` and `finalize`: if a session came back
for a rollup that was never seeded (and is not the entry chain — entry-chain
sessions are legitimate, overlay re-entry opens them there), it errors out. That
means the dispatch opened a lazy session and ran **unchained**, off this slot's
worlds — silently regressing to exactly the bug being fixed. Today it is
unreachable; it can only fire once a third rollup is wired without a slot
session. It is raised as `ExecutorErrorKind::Unavailable`, which
`sim_error_is_poison` (L408) classifies **transient**: a wiring gap is not the
transaction's fault, so the slot aborts and retries rather than evicting a
user's tx.

Helpers added alongside: `type SlotSessions` and `seed_session()` — every drain
composition seeds exactly one target chain.

The `#[tracing::instrument]` span is carried over onto `compose_chained`
(`skip_all`, fields `tx_len` + `entry_rollup_id`); the log line loses its
`simulate_and_resolve:` prefix.

*Nit (out of scope for this file, worth a follow-up):* two stale doc references
to `simulate_and_resolve` survive at `held_pool.rs:35` and `optimistic.rs:43`.

## Hunk 4 (~L267) — `MAX_BUNDLE_ATTEMPTS` doc rewrite, and the gas reserve becomes configurable

Two unrelated things in one hunk.

**(a) `MAX_BUNDLE_ATTEMPTS` doc.** The old text documented drain-time isolation
as a KNOWN LIMITATION — "a state-dependent tx whose prerequisite is co-bundled
in the same slot deterministically diverges from real execution and drops here
after the retry budget". That limitation is what this branch removes. The new
text says what the bound actually backstops now: L1 state that moves between
compose time and the bundle's inclusion block. Doc-only; the value is still 3.

**(b) `POST_BATCH_EXECUTION_GAS_RESERVE` → `DEFAULT_…` + accessor.** (This half
is the unstaged addition, made after live chiado testing.)

```rust
fn post_batch_execution_gas_reserve() -> u64 {   // OnceLock, reads EEZ_POSTBATCH_GAS_RESERVE
```

**Why:** measured on chiado, queueing ONE deferred entry inside
`postAndVerifyBatch` costs ~240k gas (a 3-entry batch bills 841k against ~126k
for a minimal one). A 24-effect postBatch therefore needs ~6M of execution
*above* the calldata floor. The fixed 4M reserve made the postBatch revert
out-of-gas **inside the block builder's bundle simulation**, and rbuilder drops
such bundles silently — the only symptom is "pinned slot built without
inclusion".

**Before → After:** 24 effects, reserve 4M → postBatch is undersized, the whole
bundle is dropped with no on-chain trace, forever. With
`EEZ_POSTBATCH_GAS_RESERVE=8000000` the same 24-effect batch settles. Default is
unchanged at 4M, so nothing moves unless the operator sets the env var. Read
once via `OnceLock`.

The durable fix (derive the reserve from the batch's entry count) is noted in
the design doc §8.3, not done here.

## Hunk 5 (~L384) — `clamp_max_postbatch_gas`: `MIN_VIABLE` const → `let`

Forced by the previous hunk: the reserve is no longer a `const`, so
`const MIN_VIABLE = RESERVE + 21_000` becomes a `let`. Same arithmetic, same
clamping behavior — except the floor now moves with the env var, which is the
point (an 8M reserve must not let a 5M `EEZ_MAX_POSTBATCH_GAS` through).

## Hunk 6 (~L393) — reserve in the clamp-failure event + doc reference rename

The `reserve = …` field on the out-of-range ERROR event reads the accessor.
`sim_error_is_poison`'s doc now names `compose_chained`. Mechanical.

## Hunk 7 (~L547) — five new drain helpers

All private, all small:

- **`restore_pool_order(Vec<(usize, HeldTx)>) -> Vec<HeldTx>`** — sorts by drain
  index and drops it. Necessary because the drain now partitions into two
  direction phases; without this, any re-queue would hand the pool back a
  permutation of what it dealt out (all outbound before all inbound), silently
  reordering the FIFO.
- **`abort_rest(failing, rest_of_phase, other_phase)`** — assembles everything a
  transient abort still owes the pool: the failing tx (absent when it was
  already evicted as poison), the remainder of the current phase, and the whole
  untouched other phase. Replaces the old inline
  `let mut rest = vec![held]; rest.extend(iter.by_ref()…)` idiom, which no
  longer covers the second phase.
- **`append_and_execute(&mut SyncBlockState, &[Bytes]) -> Option<(usize, String)>`** —
  appends txs to the block-in-progress, stopping at the first that reverts or
  fails to execute. `Some((position, why))` means the block is **half-extended**
  and must be reopened.
- **`take_l1_cache(&mut SlotSessions, rollup_id)`** — removes the L1 session,
  `checkpoint()`s it, downcasts to `revm::database::CacheState`. The payload
  shape is pinned by contract in `L1TargetSession`'s doc comment
  (`local/slot.rs:164-170`); this is the one place that depends on it, and it
  errors rather than assumes.
- **`truncated_hex`** — first 32 bytes of a revert payload for event messages.

## Hunk 8 (~L1156) — `Box::pin` at the single call site

`compose_cross_chain_batch(...)` is now boxed before `.await`. The drain holds
two live execution contexts (the L1 world and the prefix state), which pushed
the future past clippy's `large_futures` 16KB bound. No behavior change.

## Hunk 9 (~L1535) — `compose_cross_chain_batch` doc comment

Reworded: "simulate each drained transaction" → "compose the drained
transactions in canonical order over the slot's chained execution contexts"; the
cadence note now says each per-tx `finalize` is seeded with the slot's live
sessions. Doc-only.

## Hunk 10 (~L1586) — the drain's header comment

The old comment described the two-path optimistic split plus "simulate each held
tx independently". Rewritten to describe the two-phase chained drain and the
per-tx accept/evict protocol. Doc-only, but it is the map for everything below,
so worth reading in the file rather than the diff.

## Hunk 11 (~L1638) — slot execution contexts, and the drain's new bookkeeping

The substantive setup. Placement matters and is deliberate: this sits **after**
the empty-drain early return and **after** the stale-nonce partition, so the
common empty-drain slot opens no state at all, and the degrade path below
re-queues exactly the post-stale set (stale txs have already been released).

```rust
let reopen = |txs: &[Bytes]| SyncBlockState::open(l2_dyn.clone(), …, txs);
let slot_ctx = L1SlotState::open(&local.l1_entry).and_then(|world| reopen(&[]).map(…));
```

- `reopen` **rebuilds from the accepted tx list**, it does not restore a cached
  state. That is what keeps the prefix provably equal to the block the canonical
  rebuild produces — the keystone assert (hunk 18) would otherwise be comparing
  the block against a cache that drifted.
- Failure to open either context is transient: nothing has been consumed, so the
  whole (post-stale) drain goes back via `push_front_batch` and the slot degrades
  to a minimal postBatch. New event `eez.composer.phase2.slot_setup_failed`.
- Success emits `eez.composer.phase2.slot_anchored` with the pinned L1 anchor
  number + hash — the one line that tells you which L1 base every claim in this
  slot was computed against.

Three new pieces of state:

- `sync_txs: Vec<Bytes>` — the Sync block's txs in canonical order, exactly as
  accepted. This is both what the prefix is rebuilt from and what the keystone
  assert compares against.
- `system_txs_appended: u64` — a **single** SYSTEM_ADDRESS nonce cursor. It
  replaces two counters that were only ever summed at use sites, which is a
  forgot-one-term bug waiting to happen. It reproduces the canonical builder's
  two-phase allocation (outbound loads `N..N+K-1`, then inbound deliveries
  `N+K..`) **by construction**, because phase 1 runs to completion before phase
  2 starts. Note it counts *system* txs, not block txs: an accepted outbound
  contributes 2 block txs (`load`, `user`) but only 1 nonce, which is why the
  increment uses `pairs_k.len()` and not `pair_txs.len()`.
- `survivors: Vec<(usize, HeldTx)>` — drain indices ride along so re-queues can
  restore pool order across the two phases.

## Hunk 12 (~L1733) — transient payload type, and the two-phase partition

`transient` becomes `Option<(String, Vec<(usize, HeldTx)>)>` (indices). Then:

```rust
let (outbounds, mut inbounds) = drained.into_iter().enumerate()
    .partition(|(_, held)| held.direction == Direction::Outbound);
```

**Why this order, not drain order:** it is the real execution order on both
chains. On L2 the Sync block is `[load_0, user_0, …, deliver_0, …]` per
`build_cross_chain_sync_pairs`; on L1 the postBatch's inline outbound executions
run inside `postAndVerifyBatch`, which precedes the bundle's inbound user txs.
Composing in that order is what makes the chained state real rather than
plausible.

Safety of the partition: a sender's nonce chain is never reordered, because the
two directions live on different chains, and poison-gap bookkeeping is keyed per
`(sender, direction)`.

## Hunk 13 (~L1770) — PHASE 1, outbound (L2→L1)

The old outbound arm (an `if held.direction == Outbound { … } continue;` inside
one loop) becomes its own `while let Some((idx, held)) = out_iter.next()` loop.
The poison-gap pre-check at the top is unchanged (now duplicated, once per
phase).

**Contexts.** Per tx: an `L1TargetSession` over a clone of the L1 world cache
(this replays real `EEZ._processNCalls` frames — proxy auto-creation, escrow
value drawn from EEZ's balance) plus a `draft.fork()` of the block. The source
sim runs the user tx on the L2 prefix fork through `local.l2_entry` (the L2
follower client errors `Unavailable` for source sim, hence the entry client).
Failure to build either context is transient → abort with `abort_rest(Some(this),
&mut out_iter, mem::take(&mut inbounds))`.

**Unchanged gates**, kept in the same order and still evicting: zero L1 entries
(`outbound_no_entries`), more than one entry per tx
(`outbound_multicall_unsupported`), missing ether-out, over-escrow. The only
edit is the comment naming `check_entry_shape` instead of `reject_multicall`.

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

Four things to notice:

1. **`build_outbound_pair` runs `check_entry_shape` itself**, so an entry the
   Sync-block lowering cannot represent (multi-call, nested, unsuccessful,
   static/revert-span/explicit-gas) comes back as its `Err` and evicts **this
   tx** — event `cc_compose.shape_evicted`.
   **Before → After:** the same bad shape used to sail through the drain and
   blow up post-drain inside `build_cross_chain_sync_pairs`, which re-queued
   *all* survivors and degraded the slot (`phase2.sync_pairs_failed`). Next slot
   drained the same set and failed the same way: a freeze vector. Now one tx is
   evicted and the other survivors still settle.
2. **The append is a real receipt check.** If `[load, user]` does not execute on
   the block prefix, the tx is evicted (`cc_compose.append_reverted`) — it can
   never have landed on-chain either.
3. **The prefix is reopened from `sync_txs` on append failure**, because a failed
   `append_and_execute` may be half-applied (the load succeeded, the user tx reverted).
   Rebuilding from the accepted list is the only truth. If the rebuild itself
   fails, that is transient → abort.
4. **Block first, world second.** `l1_state.cache` is only overwritten after the
   pair is safely in the block. A tx evicted at append leaves the L1 world
   byte-identical to before it was tried. If the session hand-off is what fails
   (`cc_compose.l1_session_lost`, ERROR), the slot cannot chain L1 any more and
   aborts transiently — the block built so far is discarded wholesale by the
   minimal path, so the fact that the pair is in `sync_txs` but not in
   `pending_out` at that instant is inert.

Staging afterwards is unchanged (`pending_out`, `outbound_entries`, deliberately
NOT `survivor_comps`), except `pending_out.push` is now a single push of
`l1_entries[0]` rather than a loop over `l1_entries` — equivalent, since
`len() > 1` was evicted above — and `survivors.push((idx, held))`.

The poison/transient arms below are unchanged in classification; the transient
one just uses `abort_rest` and renames the error prefix to `compose_chained
outbound tx#…`.

## Hunk 13 cont. (~L2035) — PHASE 2, inbound (L1→L2)

Mirror image. Source sim runs the L1 user tx on a fork of the world
(`l1_state.fork_state`); the L2 side is an `InboundL2TargetSession` over
`draft.fork()`, seeded with the current delivery nonce cursor, which builds the
canonical delivery, executes it on the fork, and reads the claim off the real
`EEZL2 → proxy` frame.

```rust
let inbounds = if transient.is_some() { Vec::new() } else { inbounds };
```

This guard is **provably redundant today** — all four phase-1 abort paths call
`std::mem::take(&mut inbounds)`, so the vector is already empty. It is kept
deliberately as a 5-line belt: a future phase-1 abort path that forgets its
`mem::take` would otherwise double-queue every inbound tx (once via `abort_rest`,
once by falling into phase 2).

**New shape gate at accept, both halves:**

```rust
target_entries.iter().try_for_each(|e| check_entry_shape(e, "inbound"))
    .and_then(|()| /* source entries with non-empty expectedL1ToL2Calls → Err */)
```

Target-side shape first, then a scan of the *source* composition's entries for
nested `expectedL1ToL2Calls` recordings. Nested composition is parked, so a
nested recording is this tx's problem, not the slot's → evict
(`cc_compose.shape_evicted`). Because of the `and_then`, a tx malformed on both
halves reports the target-side error.

**Delivery construction is now per-tx** (`build_inbound_system_txs` at
`nonce + system_txs_appended`) with two eviction arms: an `Err` (shape rejected),
and the odd case where every entry was skipped as foreign — impossible for
own-rollup targets, so evict loudly rather than append nothing.

**The append is the verifier.** The appended delivery re-runs the exact
`RollingHashMismatch` compare that used to explode at the proof signer.

**Before → After (the #88 example):** three co-bundled `increment()` calls. Old
path: all three record `returnData = 1`, all three deliveries go into the block
unchecked, the signer sees delivery #2 reverted, refuses the window, the drain
re-queues all three, next slot repeats — permanent freeze. New path: tx#1's
probe runs on a fork of the block that already contains tx#0's delivery, so it
records `2`, and tx#2 records `3`; the block is signed. And if a claim ever
*were* wrong, the append reverts and costs that ONE tx an eviction instead of
freezing the slot.

Then, in this order: `system_txs_appended += deliveries.len()`,
`sync_txs.extend(deliveries)`, and finally `l1_state.cache = l1_fork.cache` —
the source fork's committed writes become the world so later inbound sims see
their predecessors. Same block-first/world-second discipline as phase 1. The
returned sessions are dropped (`_sessions`): the probe's fork is throwaway; only
the canonical delivery, appended to the real prefix, counts.

## Hunk 14 (~L2171) — event message wording + `survivors.push((idx, held))`

`"simulate_and_resolve produced {target_count} target(s)"` →
`"composition produced …"`. Mechanical.

## Hunk 15 (~L2193) — inbound transient arm

Uses `abort_rest(Some((idx, held)), &mut in_iter, Vec::new())` (phase 2 is last,
so there is no other phase to hand back) and renames the error prefix. Same
classification as before.

## Hunk 16 (~L2235) — restore pool order in the transient re-queue

```rust
let mut requeue = restore_pool_order(requeue);
```

**Before → After:** with a drain of `[in_A, out_B, in_C]`, the old code
re-queued in drain order. Without this line the new code would re-queue
`[out_B, in_A, in_C]` — the pool's FIFO permanently reshuffled by an
implementation detail of the drain. Now the pool gets its own order back. The
poison-cascade `retain` below is unchanged.

## Hunk 17 (~L2295) — restore pool order for survivors, post-drain

```rust
let survivors: Vec<HeldTx> = restore_pool_order(survivors);
```

Same reason, for the success path: past the drain, survivors are only ever used
for re-queue on a degrade, and the pool is owed its own order. Note this is
purely about the *pool*; the block's order is fixed by `pending_out` /
`pending_in`, which are already in canonical order. The comment above the
canonical builder is reworded "Build" → "Rebuild".

## Hunk 18 (~L2336) — THE KEYSTONE ASSERT

```rust
let canonical = interleave_sync_block_txs(&pairs);
if canonical != sync_txs { … ERROR + degrade … }
```

The block this drain appended tx-by-tx must be byte-equal to what
`build_cross_chain_sync_pairs` + `interleave_sync_block_txs` reconstructs from
the same entries — the same rebuild the deriver and the proof signer perform.
This is the single tie between the incremental construction and the canonical
one, and the reason it holds by construction is that both sides go through
`build_outbound_pair` / `build_inbound_system_txs` with the same nonce
allocation.

Inequality is a **composer bug, never bad input**, so it does not evict anyone:
loud ERROR (`phase2.canonical_mismatch`, with `first_divergent` index), re-queue
survivors, degrade to minimal. Posting a block nobody else can rebuild would be
strictly worse than posting nothing.

Note `sync_txs` is no longer computed here — it is now the drain's accumulator,
which is exactly what makes the comparison meaningful.

## Hunk 19 (~L2410) — belt: every final receipt must be success

```rust
if let Some(first_failed) = built.tx_successes.iter().position(|s| !s) { … ERROR + degrade … }
```

Every tx in the block was already receipt-verified on the very prefix
`build_sync_block` re-executes, so this should be unreachable; if it fires, the
block and the prefix disagree. Nothing in this failure class may reach the proof
signer, so: `phase2.final_receipt_failed` (ERROR), re-queue, degrade.

**Before → After:** previously a reverted system tx in the built Sync block was
not inspected here at all — it went out, and the proof signer rejected the
window (the #88 tail). Now it never leaves the composer. Depends on
`tx_successes` being surfaced from `build_sync_block` (`local/build.rs`).

## Hunk 20 (~L3310) and Hunk 21 (~L3589) — reserve accessor at the two sizing sites

`emission candidate sizing` and `sign_post_batch_tx`'s `gas_limit` both call
`post_batch_execution_gas_reserve()` instead of reading the const. This is where
the env override actually reaches the wire.

## Hunk 22–23 (~L3750, ~L3763) — `clamp_max_postbatch_gas` tests

The test's `min_viable` and the "budget at or below the reserve" case now call
the accessor. Same assertions. Note the tests are now coupled to process env: if
`EEZ_POSTBATCH_GAS_RESERVE` is set in the test environment they still pass
(everything is expressed relative to the accessor), but the `OnceLock` means the
first reader in the process fixes the value for all of them.

---

## Observable behavior changes in this file

| Change | Before | After |
|---|---|---|
| Claim computation for co-bundled txs | each tx simulated in isolation on the same pre-slot state; 3× `increment()` all claim `returnData=1` | each tx simulated on a fork of the L1 world + Sync block already containing its predecessors; claims are `1, 2, 3` |
| A wrong claim's blast radius | delivery reverts on-chain → signer rejects the window → whole set re-queued → permanent freeze | append reverts at compose time → that ONE tx is evicted, the rest of the slot settles |
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


Scope of this section, in review order:

1. `crates/eez-node/tests/chained_interstate.rs` — new file (staged)
2. `crates/eez-node/tests/common/mod.rs` — harness diff
3. `contracts/src/Counter.sol` — new file, **untracked**
4. `crates/eez-node/src/main.rs` + `crates/eez-node/src/ingress.rs`
5. `.github/workflows/ci.yml` + `docker-compose.chiado-node.yml` (both unstaged)

I read `docs/CHAINED-INTERSTATE-DESIGN.md` end to end (the verification list is
**§9**, not §8 — §8 is the chiado findings; see the nit in 1.1), skimmed
`crates/eez-node/tests/cross_chain.rs` for house style, and ran
`cargo check -p eez-node --tests` — clean, exit 0, no warnings from our crates.

---

## 0. Read this first — the commit as staged does not build in CI

`contracts/src/Counter.sol` is **untracked** (`?? contracts/src/Counter.sol`).
All four new tests call `deploy_counter`, which reads
`contracts/out/Counter.sol/Counter.json` — a `forge build` artifact.
`contracts/out/` is gitignored (`.gitignore:5`), so the artifact is never
committed; it is regenerated by the `forge build` step CI already runs before
the e2e jobs.

That means: locally everything passes because `contracts/out/Counter.sol/Counter.json`
exists on this machine from an earlier build. On a fresh CI checkout of the
staged tree, `forge build` compiles a `src/` that has no `Counter.sol`, no
artifact is produced, and `deploy_counter` fails at
`read …/contracts/out/Counter.sol/Counter.json` — **all four tests fail in
setup**, with an error that looks like a build problem rather than a missing
file.

Fix: `git add contracts/src/Counter.sol` into the same commit as the tests.
Same story, lower stakes, for the two unstaged config files: `.github/workflows/ci.yml`
(without it the new binary never runs in CI at all) and
`docker-compose.chiado-node.yml`.

---

## 1. `crates/eez-node/tests/chained_interstate.rs` (new, 644 lines)

The four tests pin the four distinct properties of the issue-#88 fix. They are
genuinely different properties, not four flavours of the same assertion —
worth saying explicitly, because at a glance they all look like "send some
cross-chain txs and check a counter":

| Test | Property pinned |
|---|---|
| `three_order_dependent_inbound_calls_in_one_bundle` | claims chain (1, 2, 3) in one drain — the literal issue repro |
| `mixed_direction_state_chain_in_one_slot` | canonical two-direction block order (outbound before inbound) |
| `poison_mid_bundle_leaves_survivors_correct` | eviction isolation (1, 2 — not 1, 3) + no window freeze |
| `same_sender_outbound_chain` | same-sender outbound nonce chain over a real L1 world (1, then 6) |

### 1.1 Module doc (lines 1–9)

States the failure precisely: isolated per-tx simulation over one pre-slot state
produces claims the chain contradicts → `RollingHashMismatch` → signer rejects
the window → the drain re-queues the same set forever. This is the right
framing for a test file: it says what a red test *means*, not what the code does.

Nit: it cites `docs/CHAINED-INTERSTATE-DESIGN.md §8`, but the verification plan
these tests implement is **§9**; §8 is "Live-chain findings (chiado)". One-word fix.

### 1.2 The `CallResult` event declaration (lines 27–32)

A local `sol!` for `CallResult(uint256 indexed, uint256 indexed, bool, bytes)`.

I verified this against both managers: `eez-core-protocol/src/L2/EEZL2.sol:135`
and `eez-core-protocol/src/EEZ.sol:171`. The two declarations differ only in a
parameter *name* (`callNumber` vs `l2ToL1CallNumber`), so topic0 is identical —
exactly as the comment claims. Emitted at `EEZL2.sol:553` / `EEZ.sol:1181`,
i.e. the value the manager actually folds into the rolling hash. Filtering by
emitting address is the correct discriminator.

Why this matters for the review: this event is the *ground truth* half of every
test. The other half is the posted calldata. The tests compare the two.

### 1.3 `MAX_USER_TXS` / `cap_env()` (lines 34–40)

Pins `EEZ_MAX_USER_TXS_PER_BUNDLE=3` for every test in the file rather than
inheriting the composer default (`composer.rs:1125`, also 3 today).

Good call, and the comment gives the reason: if someone lowers the default to 2,
the three co-bundled `increment()` calls silently split across two drains and
the test would no longer test co-bundling. It would then fail on the
"must ride ONE postBatch" assertion rather than passing vacuously — so the pin
plus that assertion are belt-and-braces in the right direction.

Small structural oddity: `MAX_USER_TXS` is a `(&str, &str)` tuple destructured
in `cap_env()` as `MAX_USER_TXS.0` / `MAX_USER_TXS.1`. Two plain consts, or just
inlining the literal into `cap_env`, reads better. Cosmetic.

### 1.4 `posted_batches` (lines 46–65) — the strongest signal in the file

Reads every `BatchPosted` log from the deploy block, fetches the posting
transaction, and decodes its input with `decode_postbatch`
(`eez-protocol/src/entries/mod.rs:593` — `postAndVerifyBatchCall::abi_decode`).

This is what makes the tests convincing. The assertions are not against
composer-internal state, or against a log line, or against a re-derived batch —
they are against the **byte-for-byte calldata L1 accepted**. If the composer
claimed `1, 1, 1`, that is what would come back here.

Cost note: every call re-scans from `deploy_block` and issues one
`eth_getTransactionByHash` per batch. Negligible at four-batch scale; not a
pattern to copy into a soak harness.

### 1.5 `inbound_claims` (lines 67–89)

Filters entries with `proxyEntryHash != 0` (deferred = inbound consumption
entries) and decodes each `returnData` as a `uint256`.

The doc comment is honest about the weak spot: the on-chain entry is lean, so
attribution is by *direction*, not by call identity — the entry binds its call
only through `proxyEntryHash` and the call shape rides the DA sidecar. That is
sound here only because the fixture generates no cross-chain traffic other than
the test's own. Worth keeping that sentence; it is the kind of assumption that
silently breaks the day someone adds background load to `setup_cross_chain`.

The `unwrap_or_else(|| panic!(...))` on a non-32-byte return is right — a
non-uint claim is a real failure, and printing the hex makes it debuggable
(invariant 7 in test form).

### 1.6 `outbound_calls` (lines 91–103)

Mirror image: immediate entries (`proxyEntryHash == 0`), flattened over
`l2ToL1Calls`, filtered to the test's L1 target.

The target filter is load-bearing: `prepare_post_batch_raw` prepends a leading
immediate entry with no `l2ToL1Calls`, and other immediates may carry unrelated
calls. Filtering by `targetAddress` drops both cleanly.

### 1.7 `CallOutcome` / `call_results` (lines 105–137)

Decodes `CallResult` logs into `(block, tx, tx_index, success, return_data)`.
`tx_index` is what test 2 uses to assert intra-block ordering; `block` is what
test 1 uses to assert "one Sync block".

The comment "a delivery that diverged from its claim reverts, taking its logs
with it" is the key insight — absence of a log here is itself the #88 symptom,
so `delivered.len() == 3` is a meaningful assertion, not bookkeeping.

Same whole-chain scope caveat as `inbound_claims`: `from_block(0)` collects
every `CallResult` the chain ever emitted. True-by-construction today (setup
only deploys contracts and creates proxies), but it is a coupling to the fixture.

### 1.8 `assert_receipt_ok` / `wait_for_count` / `assert_reconciled` (lines 139–174)

Three thin wrappers over the house `wait_for` + `SETTLE_TIMEOUT` (3 min).
`assert_reconciled` is verbatim the reconciliation poll from `cross_chain.rs`
(L1 `rollups[rid].stateRoot` vs the L2 **safe** block root) — correctly using
`safe`, since the unsafe head is optimistic.

One inconsistency worth a decision: `wait_for_count` reads the counter at
`latest` (alloy's default block tag), i.e. the *optimistic* head, while design
§8 explicitly says "when reading L2 effects for verification, use the `safe`
block tag — the unsafe head is optimistic and can roll back with a failed
bundle." In practice the tests are still sound, because the load-bearing
assertions that follow (`inbound_claims` from posted calldata, plus
`assert_reconciled`) only pass on settled state — `wait_for_count` is really a
progress gate, not a verification. But it is the one place the file diverges
from its own design doc's advice, and if it ever flakes, this is why. Either
switch it to `safe` or say in one line that it is deliberately a gate.

### 1.9 `open_drain_window` (lines 176–195) — how co-bundling is made deterministic

This is the mechanism the whole file rests on, so it deserves scrutiny.

It snapshots `batches_posted`, waits for that count to *rise*, and returns
immediately. The argument: the composer keeps at most one postBatch in flight,
so a fresh `BatchPosted` means the next drain is gated on the deriver clearing
that batch — never sooner than the next L1 block, which is 5s on the embedded
testing L1 (`TESTING_L1_BLOCK_TIME`, `eez-node/src/l1_embedded.rs:29` — verified).
Submitting the txs takes milliseconds against that.

Two things make this acceptable rather than "hopeful":

- The window is wide (seconds vs milliseconds), so it is not a tight race.
- **Every test then asserts its claims arrived in exactly one posted batch.**
  A missed window produces `claimed.len() == 2` and a failure message that says
  so explicitly ("split batches mean the drain window was missed, not that the
  invariant broke"). That is the correct design: the test cannot silently
  weaken itself into a no-op.

Nit: the doc says "Admitting three transactions at the ingress front takes
~100ms"; measured submissions are ~11ms. Harmless either way, but numbers in
comments should be the measured ones.

### 1.10 `batches` / `assert_no_evictions` (lines 197–209)

`batches` is a one-line convenience over `posted_batches`.
`assert_no_evictions` asserts zero log lines containing `"evicting"` or
`"evicted"`.

Known limitation, same baseline as `cross_chain.rs:112`: this is a substring
match over the raw log, so any line that merely contains the word counts. The
composer's eviction messages do contain it
(`composer.rs:1976` "…evicting", `composer.rs:2183` "…evicting — it can never
compose"), so the check does fire on real evictions — it is over-broad, not
under-broad, which is the safe direction. If you want to tighten it later, match
the structured event names (`eez.composer.cc_compose.poison_evicted`,
`…shape_evicted`, `…poison_chain_evicted`) instead of English.

### 1.11 Test 1 — `three_order_dependent_inbound_calls_in_one_bundle` (lines 211–312)

**The literal issue #88 repro.** Deploy `Counter` on L2, create the L1-side
cross-chain proxy for it, open a drain window, then push three `increment()`
calls from one sender at nonces n, n+1, n+2 through the L1→L2 ingress front.

What it then asserts, in order of strength:

1. All three L1 user txs landed with status 1, and L2 `count() == 3`.
2. **The claim chain, decoded from the posted `postAndVerifyBatch` calldata,
   is `[1, 2, 3]`** — and rides exactly one batch.
3. The three L2 deliveries all succeeded, share one block, and their
   `CallResult.returnData` values are `[1, 2, 3]` — i.e. what the chain actually
   computed equals what the composer claimed.
4. Reconciled L1 root == L2 safe root; zero evictions; zero
   `"local L2 state root"` divergence lines; no process death.

Assertion 2 is the one that would have caught the bug: pre-fix, all three
entries carry `returnData = 1`, the second delivery folds `2`, reverts
`RollingHashMismatch`, the signer rejects, nothing settles — so in practice the
test would fail earlier, at `wait_for_count(… 3 …)` timing out after 3 minutes.
Assertion 2 is what makes the *failure diagnosis* unambiguous rather than "it
hung".

Assertion 3 is not redundant with 2: 2 proves what was claimed, 3 proves the
chain agreed. Both are needed to state "claims are exact".

Small thing: the loop uses a manually incremented `nonce` seeded from
`pending_nonce` (which reads `getTransactionCount` at latest). That is correct
here — held txs have not landed on L1 yet, so re-reading between submissions
would hand back the same nonce three times. Worth knowing when copying this
pattern.

### 1.12 Test 2 — `mixed_direction_state_chain_in_one_slot` (lines 314–436)

Deploys a `Counter` on *each* chain, wires an L1-side proxy to the L2 counter
and an L2-side proxy to the L1 counter, then submits one inbound `increment()`
and one outbound `add(5)` into the same drain window.

Assertions:

- Both user txs succeed; L2 count 1; L1 count 5.
- Both directions ride **one** postBatch: `inbound_claims == [1]` and
  `outbound_calls == [add(5) calldata]`.
- **Canonical block order**, read from receipts: the outbound user tx and the
  inbound delivery share a block, and
  `outbound_receipt.transaction_index < delivered[0].tx_index`.
- L1's own `CallResult` (emitted by `EEZ`, inside `postAndVerifyBatch`) returned
  `5` — the outbound call really executed on L1, not just got carried.

The ordering assertion is the point of this test and it is pinned the right way:
from receipts, against `build_cross_chain_sync_pairs`' documented order
(all outbound `[load_k, user_k]` pairs, then all inbound deliveries — design §3).
A regression that flips the two passes in the drain shows up here immediately.

The `.filter(|o| o.success)` before mapping L1 return data is fine: a failed call
is dropped from the vec, so the comparison to `vec![5]` still fails. It reads as
if it might mask a failure; it doesn't.

### 1.13 Test 3 — `poison_mid_bundle_leaves_survivors_correct` (lines 438–559)

Three txs into one window: `increment()` (survivor), a poison tx, `increment()`
(survivor).

The poison is the harness's established form — a cross-chain submission whose
`to` is a plain address (`w.recipient`, `0x2222…`) rather than a proxy. I traced
the composer path: the source sim records no cross-chain call → `EmptyCalls` →
`sim_error_is_poison` → `eez.composer.cc_compose.poison_evicted` at
`composer.rs:2181-2189`, message "…evicting — it can never compose, resubmit
required". So the eviction is in-drain and deterministic, not a 3-strike
post-dispatch eviction. Good: the test does not depend on retry timing.

The sender choice is the subtle part and the comment explains it: poison
bookkeeping evicts the *rest of that sender's `(sender, direction)` nonce chain*
(`push_poison_root` / `poison_chain_evicted`, `composer.rs:2009`). If the poison
shared `INBOUND_USER` with the survivors, the second `increment()` would be
evicted as a gapped successor and the test would be measuring the wrong thing.
Hence the new `ANVIL_KEY_5`.

Assertions:

- Both survivors succeed; L2 count 2.
- **`claimed[0] == [1, 2]`, one batch** — "survivor claims must close over the
  evicted tx". This is the property: `[1, 3]` would mean the evicted tx's
  simulated effect leaked into its successor's claim. Exactly the right shape to
  pin, and it is only checkable because the claims are decoded from calldata.
- The poison has **no L1 receipt** — dropped, not bundled.
- At least one eviction was logged (the mirror of `assert_no_evictions`).
- **No freeze**: a fourth `increment()` in a *later* window settles (count 3).

That last block is the #76-adjacent property and it is the one I would have
asked for if it weren't there. Without it, "the poison was evicted" is
compatible with "and the window degraded forever after."

Two honest limitations to note:

- `receipt_ok(poison) == None` is a point-in-time check. It proves the poison
  had not landed *by then*; nothing proves it can never land later. Combined
  with the eviction log assert this is fine, but the two assertions are jointly,
  not individually, sufficient.
- This test does not assert the `"local L2 state root"` divergence line count
  (test 1 does). Since it deliberately drives an eviction path that rebuilds the
  block prefix, it is arguably the test that most wants that check.

### 1.14 Test 4 — `same_sender_outbound_chain` (lines 561–644)

Two outbound calls from **one** L2 sender at nonces n and n+1 —
`increment()` then `add(5)` — against a stateful L1 target, in one slot.

`wait_for_count(l1, 6)` is the whole story in one line: 0 → 1 → 6. Under the old
model both source sims see the same L1 snapshot (count 0) and claim `1` and `5`;
`postAndVerifyBatch` re-executes them sequentially, folds `1` and `6`, and
reverts.

Assertions:

- Both L2 user txs succeed; L1 count 6.
- Both calls ride **one** postBatch, in submission order —
  `[increment calldata, add(5) calldata]` — "sender nonce order must survive the
  drain". This pins the FIFO-per-direction guarantee from design §3.
- L1's `CallResult` return values are `[1, 6]`, i.e. the L1 world really advanced
  between the two source simulations (design §4 step 6: commit the
  `L1TargetSession` fork into the L1 world on accept).

This is the test that specifically covers `L1SlotState` advancement; tests 1 and 3
cover the L2 block-prefix side. Together they cover both halves of "the world
advances only by real frames."

### 1.15 Cross-cutting suggestions for the file

Nothing blocking, but three cheap additions would raise the file's value:

1. **Assert the drain's own invariant events never fired.** Design §8 calls out
   `canonical_mismatch`, `final_receipt_failed`, `l1_session_lost` as
   "bug, not input condition" events — they exist at `composer.rs:2353`,
   `composer.rs:2418`, `composer.rs:1940`. A single shared helper
   (`log_count_matching(&["canonical_mismatch", "final_receipt_failed",
   "l1_session_lost"]) == 0`) added to all four tests would turn silent
   degradations into failures. The keystone byte-for-byte `sync_txs` equality
   assert is exactly the sort of thing you want a CI signal on.
2. **Apply the divergence check uniformly.** Only test 1 checks
   `"local L2 state root"`. Folding it (and 1) into one `assert_clean(&w)` used
   by all four keeps them consistent.
3. `assert_no_evictions` and its inverse in test 3 could share the same pattern
   list so they can never drift apart.

---

## 2. `crates/eez-node/tests/common/mod.rs` (diff)

### 2.1 `ANVIL_KEY_5` (hunk 1)

New constant with a comment explaining *why it exists* rather than what it is:
eviction cascades along a sender's nonce chain, so poison needs its own sender.
Keys 1–4 are already spoken for (`TARGET_DEPLOYER = ANVIL_KEY_3`,
`INBOUND_USER = ANVIL_KEY_2`, `OUTBOUND_USER = ANVIL_KEY_4`).

Verified: the key derives to `0x9965507D1a55bcC2695C58ba16FB37d819B0A4dc`
(anvil account #5) and that address is prefunded in reth's dev genesis
(`crates/chainspec/res/genesis/dev.json`), which is what the embedded testing L1
uses. So the poison tx is fundable and will be admitted — which the test needs,
since it must fail at *composition*, not at ingress.

### 2.2 The port registry (hunks 2–4) — the one real behavior change in this file

**Before.** `free_port()` bound `127.0.0.1:0`, read the port, dropped the
listener, returned it — a pure availability probe with no memory.
`NodeHandle::spawn` kept its *own* local `HashSet` so one node's ~14 listeners
wouldn't collide with each other. The two used-port sets were independent.

**After.** One process-wide `LazyLock<Mutex<HashSet<u16>>>` (`HANDED_OUT_PORTS`)
that every probe inserts into. `free_port()` is now
`probe_unique_tcp_port(&mut handed_out_ports())`, and `spawn` takes the shared
guard instead of a fresh set.

**Why.** A real observed flake: the 4th sequential test in one process failed
*in setup* with "address already in use". The OS happily re-offers an ephemeral
port that an earlier node in the same process already bound — its sockets may
still be lingering (TIME_WAIT, or the child simply still holding them) when the
next node tries the real bind. With ~14 ports × 4 nodes in one binary, that
collision stops being theoretical. Sharing one set across every probe in the
process removes the re-hand entirely.

Details worth noting:

- `handed_out_ports()` uses `PoisonError::into_inner`, so a panicking test does
  not poison the registry for the tests that follow. Correct choice for a test
  harness — a poisoned mutex here would convert one real failure into three
  cascading setup failures.
- The explicit `drop(used_ports)` after the ten probes in `spawn` is
  load-bearing, not tidiness: `std::sync::Mutex` is not reentrant, so anything
  later in that long function calling `free_port()` would self-deadlock. Holding
  the guard across the ten probes also serializes concurrent spawns, which is
  free given the nextest serial config.
- The set only grows. Bounded by ports-per-process (tens), so this is fine, and
  a test process is short-lived.
- Theoretical: `probe_unique_*` loop forever if the OS only ever offers
  already-handed-out ports. Not reachable at these numbers.

### 2.3 `CrossChainConfig::new` port selection (last hunk)

**Before.** `l1_http_port = free_port()`, then `l1_auth_port = free_port()` in a
retry loop that re-rolled while it equalled `http` or `http + 1` — an ad-hoc
guard for the L1's implicit WS listener at `http + 1`. It protected only against
*that one* port, and only for *that one* draw: any later probe in the process
could still hand out `http + 1`.

**After.** `l1_http_port = probe_unique_http_port(&mut handed_out_ports())`,
which (pre-existing helper, `mod.rs:147`) verifies `http + 1` is actually
bindable and inserts **both** into the shared set; then a plain `free_port()` for
auth, which now cannot collide because both are already recorded.

Strictly stronger and shorter: the reservation is global instead of local, and
the ad-hoc loop disappears. Same helper `NodeHandle::spawn` already used for its
own L1 HTTP port, so the two paths now agree.

### 2.4 `ICounter` in the shared `sol!` block

Three functions: `count()` view, `increment() → uint256`, `add(uint256) → uint256`.
`#[sol(rpc)]` so `counter_count` can call it through a provider. Sits alongside
`IValue` / `ISetterWrapper` — consistent with the house pattern.

### 2.5 `deploy_counter` / `counter_count`

`deploy_counter` is a three-line wrapper over `deploy_raw` with no constructor
args. The reason these live in `common/` rather than in the test file is
mechanical and correct: `deploy_raw` is private to the harness. `counter_count`
is the same shape as the existing `value_no_ret` / `l2_value` helpers.

Note `deploy_counter` takes `rpc_url` and `chain_id` separately, so the same
helper deploys on L1 and L2 — tests 2 and 4 use both. Good.

---

## 3. `contracts/src/Counter.sol` (new, UNTRACKED)

```solidity
contract Counter {
    uint256 public count;
    function increment() external returns (uint256 newCount) { count += 1; return count; }
    function add(uint256 x) external returns (uint256 newCount) { count += x; return count; }
}
```

19 lines, no events, no access control, no constructor.

**Not staged.** See §0 — this is the one thing that must change before the
commit is coherent, because the staged tests read its build artifact.

**Why it exists.** It is the minimal *state-dependent* target: each call's
return value depends on its predecessors within the same block, which is exactly
the shape issue #88 describes. The existing `Value.sol` setter is also
order-dependent (`setValue` returns `(changed, newValue)`), so it *could* have
been used — but the expected claim sequence would then be a list of
`(bool, uint256)` tuples that a reader has to simulate mentally. With a counter,
"the claims must be 1, 2, 3" is readable at a glance, and a broken chain reads
as "1, 1, 1" rather than a tuple diff. That is a real reviewability win for the
file's headline test, and it is worth the 19 lines.

**Wiring.** Identical to `Value.sol`: no checked-in artifacts, `contracts/out/`
gitignored, the harness reads `contracts/out/Counter.sol/Counter.json`, and CI
already runs `forge build` in `contracts/` before both e2e jobs. I checked for
an artifact-name collision (foundry keys artifacts by *file* name): the only
other counter in scope is the submodule's `CounterContracts.sol`, so no clash.
`pragma ^0.8.28` is compatible with the pinned `solc = "0.8.34"`.

Style nit: `Value.sol` uses natspec (`/// @title`, `/// @notice`); `Counter.sol`
uses a free-form `///` block. Cosmetic, but the neighbouring file sets a
convention.

---

## 4. `crates/eez-node/src/main.rs` and `crates/eez-node/src/ingress.rs`

### 4.1 L1 entry client: bind concrete, erase after (main.rs:519–558)

**Before.** The `match l1_variant` arms each built a `LocalChainClient`, cloned
it into a `Arc<dyn ChainClient>`, and the match *evaluated to the erased view*.
The concrete handle went out of scope inside the arm.

**After.** The match evaluates to `Arc<LocalChainClient>` (the arms just return
the constructor result), and the erasure happens once, after the match:
`let entry_client_view: Arc<dyn ChainClient + Send + Sync> = l1_entry_client.clone();`

**Why.** The same instance now has to serve two consumers: the wiring's
`rollups` map wants it erased, and the new `LocalComposeClients` wants it concrete
(so the drain can reach `L1SlotState` / `simulate_source_tx_on`). They must be *one*
instance because they share a single overlay channel — two separate clients
would mean two overlay worlds and the chained drain would silently lose effects.
The new comment says exactly this.

Bonus: each arm loses four lines of `Arc<dyn …>` turbofish ceremony, so the two
arms now differ only in how they build the provider/EvmConfig, which is the
actual difference between them.

### 4.2 L2 entry client (main.rs:576–585)

**Before.** `let l2_entry = …; let l2_entry_view: Arc<dyn ChainClient …> = l2_entry;`
— erased and stored on the wiring as `l2_entry_client`.

**After.** `let l2_entry_client = …` (concrete, `Arc<LocalChainClient>`), and the
erased binding is gone entirely along with the wiring field it fed. That field
was orphaned by the drain rework: nothing reads `CrossChainWiring.l2_entry_client`
any more (I grepped — the only remaining hits are the two lines in main.rs itself).

The trailing comment was reworded from "the outbound source-sim
`simulate_and_resolve_recorded_for`" to "the outbound source simulation" —
the named function no longer exists.

**One accuracy point on the new field's doc.** `CrossChainWiring.local` is
documented (`composer.rs:140-142`) as "The same instances registered in
`rollups`; they share one overlay channel." That is true for L1
(`entry_client_view` is a clone of `l1_entry_client`) but **not** for L2:
`rollups` registers the `Role::Follower` client (`l2_follower_view`), while
`local.l2_entry` is a separately constructed `Role::Entry` client over the same
provider. Functionally fine — L2 target execution goes through
`InboundL2TargetSession`, not through the erased client, and the follower client
deliberately errors `Unavailable` for source sims — but the comment overclaims.
Worth a five-word correction so the next reader doesn't go hunting for a shared
identity that isn't there.

### 4.3 `wired_rollups` insert (main.rs:604–605)

`Arc::clone(&entry_client_view)` → `entry_client_view`: the extra clone is no
longer needed now that nothing else consumes the erased view. Mechanical.

### 4.4 `CrossChainWiring` construction (main.rs:693–701)

`entry_client` and `l2_entry_client` fields drop out; the new `local:
LocalComposeClients { l1_entry, l2_entry }` replaces them. Note `local` is a
**required** field, not an `Option` — cross-chain composer mode always has an
embedded L1 (this whole block is inside the `if embedded L1` arm), so there is no
"wired but no local handles" state to represent. Correct call: an `Option` here
would have created an unreachable `None` branch that the drain would have to
handle.

### 4.5 `ingress.rs` (comment only)

One doc line: `simulate_and_resolve` (deleted) → "the composer's chained
simulation". Same rewording as main.rs. The rewrap leaves a ragged
"…effect). One front / per source chain (invariant 8)." — reflow when you next
touch it.

---

## 5. CI and compose

### 5.1 `.github/workflows/ci.yml` (unstaged)

One line: `--test cross_chain` → `--test cross_chain --test chained_interstate`
in the existing `cross-chain-e2e` job. Without it the new binary compiles in CI
but never runs.

Two things I checked so you don't have to:

- The job already runs `forge build` in `contracts/` before the tests, so the
  `Counter.json` artifact is produced — **provided `Counter.sol` is committed**
  (§0).
- `.config/nextest.toml` serializes by `filter = "kind(test)"`, so the new binary
  is covered by the existing integration test-group automatically; no config
  change needed. The explicit `--test-threads=1` is belt-and-braces.

Side note, not introduced here: `cargo test --workspace` (the pre-commit gate in
CLAUDE.md) now compiles and runs *two* heavy node-spawning binaries in parallel,
which is precisely what the nextest config exists to prevent. Pre-existing with
`cross_chain`; adding a second one makes it more likely to bite. Worth a line in
the contributor docs at some point.

### 5.2 `docker-compose.chiado-node.yml` (unstaged, operational)

Three parameterizations, all host-env overridable, **all defaults unchanged** —
so a node started with no extra env behaves exactly as before. These come
straight out of the live chiado runs (design §8).

**a. `EEZ_SIGNER_PORT` (3 sites).** Before, `50061` was hardcoded three times:
the signer's `EEZ_PROOF_SIGNER_ADDR`, its healthcheck `nc -z`, and the node's
`EEZ_PROVER_URL`. After, all three read `${EEZ_SIGNER_PORT:-50061}`.

Why: on this host, 50061 *and* 50062 were squatted by leftover signer/proverd
processes from other worktrees, and with the port hardcoded in three places
there was no way to move the stack without editing the file. The three sites
must move together — which is the argument for a single variable rather than
three overrides.

**b. `EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS`
(`${EEZ_SIGNER_MAX_CHECKPOINTS:-8}`).** New passthrough. I verified the signer's
own default is 8 (`eez-proof-signer/src/config.rs:133-139`), so the compose
default is a no-op.

Why it needs a knob: the signer's stateless validator caps per-window
transaction-state checkpoints. A window with more effects than the cap fails
`prepare_post_batch_raw` **deterministically**, so the drain degrades and
requeues the same window forever — the #76 blind-spot shape one layer up
(design §8 finding 2). The knob must be sized ≥ the effect count implied by
`EEZ_MAX_USER_TXS_PER_BUNDLE`. The chiado run used 64 with a 24-tx bundle.

**c. `EEZ_POSTBATCH_GAS_RESERVE` (`${EEZ_POSTBATCH_GAS_RESERVE:-4000000}`).**
Passthrough for the composer knob (`composer.rs:293-303`,
`DEFAULT_POST_BATCH_EXECUTION_GAS_RESERVE = 4_000_000`), so again a no-op at
default.

Why: each deferred entry costs ~240k gas to queue inside `postAndVerifyBatch`,
so a many-effect batch blows past a fixed 4M reserve, reverts **out of gas inside
the builder's bundle simulation**, and rbuilder drops the bundle *silently*
("pinned slot built without inclusion"). That is the worst failure shape there
is — no error anywhere, just non-inclusion. The chiado run used 8M.

Two review points on this file:

- **Comment placement bug.** `EEZ_POSTBATCH_GAS_RESERVE` is inserted *between*
  the three-line comment "Max user_txs bundled per postBatch. rbuilder-chiado
  partial-includes beyond ~3 …" and the `EEZ_MAX_USER_TXS_PER_BUNDLE` line it
  describes. As written, the comment now reads as documentation for the gas
  reserve. Move the new line above the comment block, or give it its own
  one-liner — it deserves one anyway, since "under-reserved postBatch → silent
  bundle drop" is not guessable.
- **Naming asymmetry.** `EEZ_POSTBATCH_GAS_RESERVE` uses the same name on host
  and container; the checkpoint knob uses `EEZ_SIGNER_MAX_CHECKPOINTS` on the
  host for `EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS` in the container.
  Defensible (the real name is a mouthful) but inconsistent — an operator
  exporting the long name will silently get the default.
- Nothing enforces the couplings these three knobs have with
  `EEZ_MAX_USER_TXS_PER_BUNDLE` (checkpoints ≥ effects, reserve ≳ 240k × entries).
  Design §8 names the durable fixes — cap the drain at the prover's quota, derive
  the reserve from entry count. A pointer to §8 in the compose comment would save
  the next operator the two-hour debug both findings cost.

---

## 6. Verification status

- `cargo check -p eez-node --tests` — clean.
- Author ran the four tests: 4/4 pass, ~350s wall, with the pre-existing suites
  re-verified.
- Honest limitation, unchanged from the existing baseline: the eviction
  assertions are English-substring matches over the node log
  (`"evicting"` / `"evicted"`), the same technique `cross_chain.rs` already uses.

---

## 7. Behavior-change inventory

| Change | Before | After |
|---|---|---|
| `crates/eez-node/tests/chained_interstate.rs` | no coverage of chained-interstate composition | 4 e2e tests pinning claim chaining, two-direction block order, poison isolation + no-freeze, same-sender outbound nonce chains |
| Claim verification technique | effects checked via chain state / logs | claims decoded from the actual posted `postAndVerifyBatch` calldata and compared against the manager's own `CallResult` events |
| Co-bundling determinism | n/a | `open_drain_window` aligns on a fresh `BatchPosted` (≥5s of headroom); every test asserts its claims rode ONE batch, so a missed window fails loudly |
| `ANVIL_KEY_5` | keys 1–4, all assigned to roles | 5th prefunded sender available; poison gets its own `(sender, direction)` nonce chain |
| `ICounter` / `deploy_counter` / `counter_count` | no stateful counter helper | shared harness helpers (needed here because `deploy_raw` is harness-private) |
| **Port handling — `free_port()`** | stateless availability probe; `NodeHandle::spawn` kept a *separate* local used-set → the 4th node in a process could be re-handed a port an earlier node's lingering sockets still held, failing setup with "address already in use" | one process-wide `HANDED_OUT_PORTS` registry shared by every probe; a port is never handed out twice in a process; poison-tolerant lock so one panicking test doesn't cascade |
| **Port handling — `CrossChainConfig::new`** | `free_port()` for HTTP, then a retry loop re-rolling auth until it differed from `http` and `http+1`; the implicit WS port was never reserved against later probes | `probe_unique_http_port` verifies and reserves both `http` and `http+1` in the shared registry; auth is a plain `free_port()` that cannot collide; retry loop deleted |
| `contracts/src/Counter.sol` | no order-dependent fixture target with a scalar return | 19-line counter; `increment()`/`add()` return the new count, making the expected claim chain `1, 2, 3` readable at a glance |
| `main.rs` L1/L2 entry clients | erased to `Arc<dyn ChainClient>` inside the match; concrete handle discarded | concrete `Arc<LocalChainClient>` bound first, erased once afterwards, so the wiring map and `LocalComposeClients` share one instance (one overlay channel) |
| `CrossChainWiring` fields (from main.rs's side) | `entry_client` + `l2_entry_client` (erased); `l2_entry_view` binding | both gone; required `local: LocalComposeClients { l1_entry, l2_entry }` |
| `main.rs` / `ingress.rs` comments | referenced the deleted `simulate_and_resolve` | reworded to "the composer's chained simulation" (comment-only) |
| `.github/workflows/ci.yml` | cross-chain job ran `--test cross_chain` | also runs `--test chained_interstate` (same serial job, same `forge build` prerequisite) |
| **compose: signer port** | `50061` hardcoded in 3 places (signer addr, healthcheck, `EEZ_PROVER_URL`) — unmovable when the port is squatted by another worktree's processes | `${EEZ_SIGNER_PORT:-50061}` in all 3; default identical, whole stack relocatable with one export |
| **compose: signer checkpoint quota** | not set → signer's built-in default of 8; a window exceeding it fails `prepare_post_batch_raw` deterministically and the drain requeues forever | `EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS: ${EEZ_SIGNER_MAX_CHECKPOINTS:-8}`; default identical, raisable to match the bundle cap |
| **compose: postBatch gas reserve** | not set → composer's 4M default; a many-effect batch (~240k gas per deferred entry) reverts OOG inside the builder's bundle simulation and is dropped silently | `EEZ_POSTBATCH_GAS_RESERVE: ${EEZ_POSTBATCH_GAS_RESERVE:-4000000}`; default identical, sizable to the bundle cap |

### Must-fix before commit

1. `git add contracts/src/Counter.sol` — the staged tests depend on its build artifact; without it CI fails all four in setup.
2. Stage `.github/workflows/ci.yml` (otherwise the new tests never run in CI) and `docker-compose.chiado-node.yml`.
3. Module doc: `§8` → `§9` (verification).

### Worth doing

4. Move the `EEZ_POSTBATCH_GAS_RESERVE` line out from between the `EEZ_MAX_USER_TXS_PER_BUNDLE` comment and its variable.
5. Add `canonical_mismatch` / `final_receipt_failed` / `l1_session_lost` to a shared "clean run" assertion used by all four tests, and apply the `"local L2 state root"` check uniformly.
6. Correct the `CrossChainWiring.local` doc: the shared-instance claim holds for L1, not for L2 (which is a distinct `Role::Entry` client).
7. `wait_for_count` reads at `latest`; design §8 says read effects at `safe`. Either switch it or note that it is a progress gate, not a verification.


---

## Appendix A — live-chiado incidents behind the three "late" hunks

Three hunks exist only because the change was validated on live chiado
(fresh deploy, real rbuilder relay) after the dev e2e suite was already
green. Each is a dev-environment blind spot worth knowing about:

1. **`frame_gas` clamp** (`slot.rs`). Chiado's L1 block gas limit is
   ~17M; `DIRECT_CALL_GAS_LIMIT` is 30M. revm rejects a tx whose gas
   limit exceeds the block's, so every manager frame failed
   ("caller gas limit exceeds the block gas limit") and every outbound
   composition poison-evicted. Dev chains have ≥30M blocks and never see
   this. Clamping to the anchor block's limit was chosen over the old
   session's blanket `disable_block_gas_limit` because it matches what
   the real chain enforces.
2. **`EEZ_POSTBATCH_GAS_RESERVE`** (`composer.rs` + compose file).
   Measured on chiado: queueing one deferred entry inside
   `postAndVerifyBatch` costs ~240k gas (a 3-entry batch used 841k vs
   ~126k for a minimal one). A 24-effect batch therefore needs ~6M of
   execution above the calldata floor — the fixed 4M reserve made the
   postBatch revert **out of gas inside the block builder's bundle
   simulation**, and rbuilder drops such bundles silently. The relay has
   no bundle tx-count limit (confirmed: it included a 25-tx bundle once
   the request was honest); every "relay drop" we chased was our own
   under-gassed request. The reserve is now env-tunable; the durable fix
   (tracked) is deriving it from the batch's entry count.
3. **Signer checkpoint quota + signer port** (compose file). The proof
   signer caps per-window transaction-state checkpoints at 8 by default;
   a 24-effect window fails `prepare_post_batch_raw` deterministically
   and the drain requeues forever — the #76 blind-spot shape one layer
   up. The knob must be sized ≥ the bundle cap (parameterized alongside
   it). The signer port became parameterized because two leftover
   processes on the host squatted 50061 and 50062.

Validated end state on chiado: full matrix (both directions ×
direct/wrapper × setter/deposit/withdraw + reverts) semantically exact;
120/120 paced load; the issue-#88 repro (3 same-sender nonce-ordered
increments → results 1,2,3 in one Sync block); **24 inbound cross-chain
txs settled in ONE Sync block** (`count()==24` at the safe head, results
1..24) and **24 outbound in ONE Sync block**; zero divergence throughout;
none of the new invariant events ever fired. Operational rule learned the
hard way: verify L2 effects at the `safe` block tag — the unsafe head is
optimistic and rolls back with failed bundles.

## Appendix B — headline behavior changes

Each part ends with its full change/before/after inventory; these are the
ones that matter most when operating or reviewing the system:

| Change | Before | After |
|---|---|---|
| Co-bundled order-dependent claims | Recorded against the same pre-slot state → signer rejects the window → txs requeue forever | Recorded against the accumulated chained state → windows sign; verified claims 1..24 in one Sync block on chiado |
| Claim-mismatch blast radius | Whole slot degrades + requeues at the front (freeze) | The offending tx is evicted at append time; the slot and its survivors proceed |
| Unsupported entry shapes (nested/multicall) | Failed post-drain in the canonical builder → whole-slot degrade, forever | Shape-gated per tx at accept → precise eviction with a loud event |
| Target-side execution model | Direct call with a forged proxy `msg.sender` (+ nonce-restore hack) | The real contract paths: canonical delivery tx on the block prefix; real `EEZ → proxy.executeOnBehalf` frames with real escrow balance |
| L2 simulation base | `provider.latest()` per tx, discarded | The Sync-block-in-progress prefix (byte-exact by construction, receipt-checked) |
| L1 simulation base | `latest()` per tx, discarded | One anchor pinned per drain + commit-or-drop effect cache |
| Composer/deriver block agreement | Implicit (same builders, invoked separately) | Asserted byte-for-byte every slot (keystone assert; ERROR + degrade on mismatch) |
| `simulate_and_resolve` public API | Existed (isolated semantics) | Deleted — no unchained composition path remains |
| postBatch gas sizing | Fixed 4M execution reserve | `EEZ_POSTBATCH_GAS_RESERVE` env knob (default unchanged); needed ≈ 240k × deferred entries |
| Error precedence (both halves malformed) | Outbound shape error reported first | Inbound reported first (accepted trivial change) |
