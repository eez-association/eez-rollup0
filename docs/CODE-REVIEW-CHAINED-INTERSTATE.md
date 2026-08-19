# Chained-interstate code review findings

> `/code-review` run (high effort, multi-angle) against the working tree
> for the chained-interstate change-set (issue #88 fix — see
> `docs/CHAINED-INTERSTATE-DESIGN.md` for rationale and
> `docs/CHAINED-INTERSTATE-REVIEW.md` for the hunk-by-hunk walkthrough).
> This document is the review's findings only; it does not re-explain the
> change itself.

Findings are ranked most-severe first. Each was either confirmed by a
delivered sub-agent result or independently verified by direct source
read (`grep`/`Read` against the cited files).

## Summary

| # | File:line | Category | Finding |
|---|---|---|---|
| 1 | `contracts/src/Counter.sol:1` | ci-breakage | Test fixture contract untracked in git — breaks CI/fresh checkout |
| 2 | `crates/eez-composer/src/composer.rs:1876` | correctness | Escrow decremented before a tx's eviction is final |
| 3 | `crates/eez-composer/src/local/client.rs:104` | correctness | L2 has two split overlay-channel client instances |
| 4 | `crates/eez-composer/src/composer.rs:416` | correctness | DB errors misclassified as permanent poison |
| 5 | `crates/eez-composer/src/local/slot.rs:381` | correctness | `L1TargetSession` commits state unconditionally on revert |
| 6 | `crates/eez-composer/src/local/slot.rs:324` | correctness | Static-call rejection misclassified as transient (livelock) |
| 7 | `crates/eez-composer/src/composer.rs:2207` | correctness | Poison eviction runs before keystone/receipt safety nets |
| 8 | `crates/eez-composer/src/local/client.rs:315` | latent-risk | Dead `ChainClient::simulate_source_tx` still reachable |
| 9 | `crates/eez-composer/src/local/build.rs:461` | correctness | `SyncBlockFork.applied` counter not rewound on restore |
| 10 | `crates/eez-composer/src/composer.rs:232` | correctness | Unseeded-session dispatch classified as transient (N-rollup risk) |
| 11 | `crates/eez-composer/src/local/slot.rs:460` | duplication | `restore_nonce` duplicates `restore_caller_nonce` |
| 12 | `crates/eez-composer/src/local/build.rs:382` | efficiency | `SyncBlockState::fork` clones cache O(N²) per slot |
| 13 | `crates/eez-composer/src/local/slot.rs:484` | docs | Doc comment narrates history, violates present-tense rule |
| 14 | `crates/eez-composer/src/composer.rs:1756` | duplication | Outbound/inbound drain phases duplicate ~200 lines |
| 15 | `crates/eez-composer/src/local/slot.rs:1` | test-coverage | New slot.rs EVM wrapper machinery has zero unit tests |

## Findings

### 1. `contracts/src/Counter.sol` untracked in git — CI breakage

**File:** `contracts/src/Counter.sol:1`

The new e2e test fixture contract is untracked in git, so a fresh
checkout/CI run will not have it.

**Failure scenario:** `git status` shows `?? contracts/src/Counter.sol`;
`tests/common/mod.rs:1875-1890` deploys it via the forge build artifact
`out/Counter.sol/Counter.json`, and `.github/workflows/ci.yml:136-138`
runs `forge build` then `cargo nextest run --test chained_interstate`. On
any checkout that doesn't have this uncommitted file (a fresh clone, CI,
a teammate's machine), `forge build` never produces the artifact and all
4 new `chained_interstate` tests panic in setup — the new CI job fails
every run until this file is committed.

**Most actionable item** — one-line `git add` fix.

### 2. Escrow decremented before eviction is final

**File:** `crates/eez-composer/src/composer.rs:1876`

Outbound escrow budget (`escrow_remaining = Some(avail - need)`) is
decremented as soon as the sufficiency check passes, but the same tx can
still be evicted afterward by the shape gate in `build_outbound_pair`
(~1884) or by `append_and_execute` reverting against the live prefix (~1906), and
neither eviction path credits the debit back.

**Failure scenario:** Real L1 escrow = 10 ETH. tx A (8 ETH) passes the
check, `escrow_remaining` drops to 2 ETH, then tx A reverts on
`append_and_execute` and gets poison-evicted — the 8 ETH was never actually drawn
down on-chain. tx B (5 ETH), which the real escrow can cover, is now
wrongly rejected as `outbound_over_escrow` (5 > cached 2) and the user
must resubmit for no protocol reason.

Independently found by 4 separate finder passes reading the diff from
different angles — strongest convergent signal in this review.

### 3. L2 split across two overlay-channel client instances

**File:** `crates/eez-composer/src/local/client.rs:104`

L2 gets two independent `LocalChainClient` instances (`l2_follower`
registered in `cc.rollups`, and a separate `l2_entry_client`), each
opening its own `OverlayChannel` — unlike L1, where the entry client and
the registered rollup client are the same `Arc`, which `slot.rs`
explicitly documents as required for overlay re-entry to work.

**Failure scenario:** During the outbound (L2→L1) phase, the source-sim
inspector for `entry_rollup=L2` pushes its pre-dispatch snapshot into
`l2_entry`'s overlay_channel. If the dispatched L1 target session makes a
nested call back into L2, `CompositionBuilder` resolves it via
`cc.rollups[L2] = l2_follower`, whose `begin_execution_session()` peeks
`l2_follower`'s own overlay_channel — never pushed to — silently gets
`None`, and opens the re-entered L2 session on plain parent state instead
of the in-flight source-sim state, missing whatever the source tx already
wrote. Silent state divergence, not a hard error.

### 4. DB errors misclassified as permanent poison

**File:** `crates/eez-composer/src/composer.rs:416`

`sim_error_is_poison` treats any `ExecutorErrorKind::Evm` as permanent
poison, but every `evm.transact()` call in `local/slot.rs` (lines 132,
251, 366) routes through the same `evm_err()` helper regardless of
whether the underlying revm error was a deterministic tx rejection or a
backing-store I/O failure (`EVMError::Database`) — which the codebase's
own `Provider` variant (explicitly excluded from poison, doc'd as "reth
MDBX read" failures) exists to distinguish.

**Failure scenario:** A transient MDBX read hiccup or lock contention
during a probe/manager-frame `evm.transact()` call surfaces as
`ExecutorErrorKind::Evm`, gets classified as poison, and permanently
evicts a perfectly valid held tx instead of the slot aborting and
retrying next slot.

Independently found by 2 separate full-diff passes; confirmed by direct
code read of both `sim_error_is_poison` and every `evm.transact()` call
site.

### 5. `L1TargetSession::execute()` commits state unconditionally on revert

**File:** `crates/eez-composer/src/local/slot.rs:381`

`L1TargetSession::execute()` calls `self.state.commit(changes)`
unconditionally after `evm.transact()`, with no success check — unlike
the sibling `manager_frame()` helper (line 253), which explicitly checks
`result.result.is_success()` before committing.

**Failure scenario:** Currently masked because a reverted top-level
transact's state diff is just the sender nonce bump, which is separately
restored via `restore_nonce`. But no test pins "a reverted manager frame
leaves L1SlotState.cache byte-identical" — a future revm/reth repin (this
project has repinned before, per `CLAUDE.md`) that changes what a
reverted `transact()` returns in `result.state` would silently corrupt
the slot-shared L1SlotState cache for every subsequent tx in the drain.

### 6. Static-call rejection misclassified as transient (livelock)

**File:** `crates/eez-composer/src/local/slot.rs:324`

A static/view cross-chain call is rejected via
`ExecutorErrorKind::Unavailable`, which `sim_error_is_poison` classifies
as transient rather than poison — but "static execution unimplemented" is
a permanent, deterministic property of the tx, not a transient failure.

**Failure scenario:** A held tx resolving to a static call reaches the
front of a phase; the transient classification re-queues it (and
everything behind it in that phase) instead of evicting just that one
tx. Nothing converts the tx's call mode between attempts, so the
identical error recurs every subsequent slot, permanently blocking all
co-bundled transactions behind it in that phase.

### 7. Poison eviction runs before keystone/receipt safety nets

**File:** `crates/eez-composer/src/composer.rs:2207`

Poison-tx eviction from the `HeldPool` runs unconditionally, before the
new KEYSTONE canonical-mismatch assert (~2346) and final-receipt belt
check (~2416) that this diff adds specifically to catch composer-side
drain bugs.

**Failure scenario:** If a composer bug causes drain-time
poison-misclassification of tx A, and later in that same drain the
keystone/final-receipt check correctly fires and re-queues the other
survivors, tx A is already permanently evicted from the pool with only a
WARN log — the user silently loses a transaction that was never actually
bad, defeating the stated purpose of the new safety nets in exactly the
scenario they exist for.

### 8. Dead `ChainClient::simulate_source_tx` still reachable

**File:** `crates/eez-composer/src/local/client.rs:315`

`ChainClient::simulate_source_tx` — the plain trait method's old
chain-tip-only simulation, the exact behavior issue #88 replaced
everywhere with `simulate_source_tx_on` — is still implemented and
reachable through the type-erased `Arc<dyn ChainClient>` in `cc.rollups`,
with zero remaining callers today but no compiler signal preventing a new
one.

**Failure scenario:** A future caller reaching a client only through the
erased trait object (a new dispatcher helper, a test, or a refactor) that
calls `.simulate_source_tx(...)` instead of `.simulate_source_tx_on(...)`
silently gets un-anchored, chain-tip semantics instead of the slot's fork
state — reintroducing issue #88's failure class from a different call
site.

Independently found by 2 separate passes.

### 9. `SyncBlockFork.applied` counter not rewound on restore

**File:** `crates/eez-composer/src/local/build.rs:461`

`SyncBlockFork::restore_cache` resets `state.cache` but not the `applied`
tx-position counter, so `InboundL2TargetSession`'s probe-then-restore-then-real
sequence leaves `applied` permanently inflated relative to the fork's
actual tx count.

**Failure scenario:** Every inbound call runs PROBE (`applied+=1`) →
`restore_cache` (cache rewound, `applied` untouched) → REAL
(`applied+=1`). Currently diagnostic-only (`applied` only feeds `idx` in
`BuildError` log messages, confirmed by grep), but flagged independently
by all 5 correctness-angle passes as a real bug the moment `applied` is
used for anything position-sensitive — and it already actively misleads
debugging of real mid-drain failures today.

### 10. Unseeded-session dispatch classified as transient (N-rollup risk)

**File:** `crates/eez-composer/src/composer.rs:232`

`compose_chained`'s "unseeded session" guard classifies dispatch to a
registered-but-unseeded rollup as TRANSIENT (whole-phase abort +
re-queue) rather than poison, which is only harmless today because
exactly one L2 rollup exists.

**Failure scenario:** The moment a 3rd rollup is wired in (this
codebase's stated day-one N-rollup design, invariant 8), a held tx
legitimately dispatching there deterministically re-triggers this guard
on every retry attempt, aborting and re-queuing the entire drain forever
with no eviction path — reproducing the exact whole-slot-freeze failure
class issue #88 fixed, now for the N-rollup case.

### 11. `restore_nonce` duplicates `restore_caller_nonce`

**File:** `crates/eez-composer/src/local/slot.rs:460`

`restore_nonce` in `slot.rs` is a byte-for-byte reimplementation of the
already-existing `restore_caller_nonce` in `local/session.rs`.

**Failure scenario:** Two independent copies of the "restore a synthetic
frame's caller nonce" invariant now drift independently; a future fix
(e.g. handling a missing-account edge case) applied to only one copy
silently leaves the other path's synthetic frames diverging from real
chain nonces.

### 12. `SyncBlockState::fork` clones cache O(N²) per slot

**File:** `crates/eez-composer/src/local/build.rs:382`

`SyncBlockState::fork()` / `L1SlotState::fork_state` deep-clone the full
accumulated `CacheState` on every held tx attempted in the drain, giving
O(N²) total clone cost per slot as both the number of attempts and the
accumulated cache size grow.

**Failure scenario:** Negligible at today's default
`EEZ_MAX_USER_TXS_PER_BUNDLE=3`, but per-slot compose cost scales roughly
quadratically with bundle size — a latent throughput/slot-timeout risk if
that cap is raised, which project notes already flag as "unblocked but
not yet changed."

Independently found by 3 separate passes (Efficiency, Simplification, and
two full-diff scans).

### 13. Doc comment narrates history — violates present-tense rule

**File:** `crates/eez-composer/src/local/slot.rs:484`

A doc comment reads "...on-chain compare (EEZL2.sol:466) that used to
fire at the proof signer" — violates `CLAUDE.md` Working principle 7:
"Present tense, present code. Comments describe current behavior; history
lives in git. No 'previously / formerly / will'."

**Failure scenario:** Direct, quotable rule violation: the comment
narrates history instead of describing current behavior, exactly what
the rule prohibits.

### 14. Outbound/inbound drain phases duplicate ~200 lines

**File:** `crates/eez-composer/src/composer.rs:1756`

The outbound (~1756-1992) and inbound (~2005-2202) drain phases share an
almost identical ~200-line skeleton, including a verbatim-duplicated
"append reverted → evict + rebuild prefix" recovery block.

**Failure scenario:** A correctness fix to one phase's poison-gap or
append-failure recovery logic is easy to apply to only one of the two
copies, silently leaving the other phase inconsistent. One pass notes the
copies have already drifted slightly (the inbound rebuild-failure arm
passes an empty `Vec` where the outbound one passes real other-phase
state).

Independently found by 3 separate passes (Simplification and two
full-diff scans).

### 15. New `slot.rs` EVM wrapper machinery has zero unit tests

**File:** `crates/eez-composer/src/local/slot.rs:1`

The new hand-rolled EVM wrapper machinery this diff introduces
(`L1SlotState`, `L1TargetSession`, `InboundL2TargetSession`, `SkipTopFrame`,
`ProbeInspector`, ~749 lines) has no `#[cfg(test)]` module at all, unlike
`build.rs`'s `SyncBlockState`/`SyncBlockFork`, which has an explicit equivalence
test plus isolation/restore-rewind tests.

**Failure scenario:** The two real state-handling bugs found in this same
file (#3, the L2 overlay-channel split, and #5,
`L1TargetSession::execute()`'s unconditional commit-on-revert) both live in
this untested surface; a future refactor of `SkipTopFrame`'s depth counter
or `L1SlotState`'s restore path has no fast unit-test equivalent to
`build.rs`'s, so a regression here would only surface as an intermittent
e2e/chiado failure.

## Process notes

- Background-agent reliability was rocky this run: several forked finder
  agents ran far past their assigned scope (two independently spent 40+
  minutes and internally replicated the entire 10-angle review before
  returning a consolidated report), one hung outright for 30+ minutes and
  had to be killed and relaunched, and one intermediate synthesis step
  briefly fabricated "received" content for two angles before any real
  result had arrived (caught and corrected in-line, not reflected in the
  findings above).
- Everything in this document was either confirmed by a real, delivered
  agent result, or independently verified by direct source read
  (`grep`/`Read` against `composer.rs`, `local/slot.rs`, `local/build.rs`,
  `local/client.rs`, `.github/workflows/ci.yml`).
- `docs/CHAINED-INTERSTATE-REVIEW.md` (untracked, in-tree self-review)
  independently corroborates several of these findings, notably #1
  (`Counter.sol`) and #9 (the `applied`-counter drift).

---

## Triage & resolution (2026-08-14)

(Names in the findings above predate the 2026-08-18 naming pass:
PrefixState→SyncBlockState, PrefixFork→SyncBlockFork, L1State→L1SlotState,
L1ManagerExec→L1TargetSession, L2BlockProbeExec→InboundL2TargetSession,
LocalSlotHandles→LocalComposeClients, ExecOutcome→TxOutcome.)

Each finding verified against the code before classification. 9 fixed, 2
declined with reasons, 4 deferred with documentation. All fixes gated
(fmt / clippy `-D warnings` / unit suites) and the compose-path e2e
binaries re-run green.

| # | Verdict | Resolution |
|---|---|---|
| 1 | CONFIRMED | `contracts/src/Counter.sol` staged. |
| 2 | CONFIRMED (liveness) | Debit moved to the accept point (after `append_and_execute` succeeds); the sufficiency check stays early for cheap eviction. An eviction between check and accept no longer strands budget. Soundness was never at risk — the on-chain escrow check is authoritative. |
| 3 | CONFIRMED (latent) | Deferred + documented on `LocalComposeClients`: contained today because nested compositions are shape-gated to eviction, so the divergent sim output is always discarded; unifying the two L2 clients' channels is a prerequisite of nested support. |
| 4 | CONFIRMED | Fixed at three boundaries (one more than the finding): `slot.rs` transacts (`transact_err`: `EVMError::Database` → Provider/transient), `client.rs` `source_sim` (DB errors now `Err(provider)` instead of swallowed as `success=false` → poison), `build.rs`/drain (`BuildError::Provider` + `is_provider()`; append failures of the store class abort the slot instead of evicting). Partially pre-existing — the legacy session has the same mapping; its surface is off the drain path. |
| 5 | CONFIRMED (hygiene) | Commit gated on frame success; reverted frames drop their changes. Was masked (revert state = caller nonce bump, separately restored) but repin-fragile. |
| 6 | CONFIRMED | Static-call rejection reclassified `Unavailable` → `Encoding` (poison): a deterministic-unsupported tx now evicts once instead of re-queuing its phase forever. The classification bug predates this change-set; the new executors made it fixable at two sites. |
| 7 | DECLINED | Poison eviction is justified by the tx's own deterministic sim failure, independent of slot outcome. Coupling eviction finality to the keystone/belt would keep genuine poison cycling through every degrade, and a re-queued misclassified tx re-fails by definition. The safety nets guard the *block*, not the eviction verdicts. |
| 8 | CONFIRMED | `ChainClient::simulate_source_tx` (default impl + sole override) deleted; zero callers verified. The last erased-trait path to un-anchored simulation is gone. |
| 9 | CONFIRMED (cosmetic) | `ForkSnapshot { cache, applied }` — restore rewinds both; the build.rs restore test now asserts it. |
| 10 | VALID DIRECTION, DEFERRED | The correct fix is seeding a session for every registered rollup, which needs per-rollup execution contexts — multi-L2 phase work (invariant 8 roadmap). Until then transient-and-loud (WARN per slot, pure-L2 unaffected) beats poison, which would evict valid txs on a composer misconfiguration. |
| 11 | CONFIRMED | Single `reset_frame_caller_nonce` in `local/mod.rs`; both the legacy session and the slot executors call it. |
| 12 | ACKNOWLEDGED, DEFERRED | Measured: a full 24-tx slot composes + proves + dispatches in ~145 ms; the clone term is O(touched accounts) per fork. The genuinely super-linear term is the pre-existing `sync_block_pair_roots`, the right target if caps reach the hundreds. |
| 13 | CONFIRMED | Reworded to present tense; file swept for other history phrasing. |
| 14 | DECLINED (mostly) | The cited "drift" is not drift: phase 2 passes an empty remainder because there is no other phase after it, by construction. The phases differ exactly where it matters (session type, source context, staging, nonce cursor); a shared-skeleton refactor over 8+ captured locals was weighed in the necessity audit and belongs in its own commit if ever. |
| 15 | PARTIALLY VALID, DEFERRED | The state machinery under the executors (SyncBlockState / SyncBlockFork) is unit-tested by the equivalence suite; the executors are pinned by 6 e2e tests + the live chiado runs. The right follow-up is a MockEthProvider harness carrying real EEZ/EEZL2 bytecode, which would also let finding #5's fix get a cheap unit pin. |

Fix diff: +213/−186 across `composer.rs`, `local/{build,client,mod,session,slot}.rs`, `eez-protocol/src/executor.rs`; concrete DB-error type matched: `EVMError<EvmDatabaseError<ProviderError>>` (revm 38 BAL layer).
