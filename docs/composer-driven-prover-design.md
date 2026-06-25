# Composer-Driven Prover — design

Status: design, adversarially reviewed (2 lenses, both HOLD). Supersedes the
prover-side `last_accepted` / `root_to_height` / re-anchor cursor model.

## Problem it solves

Today the **prover** picks `from_block = last_accepted+1` (an in-memory,
corruptible cursor) and the composer passively serves `snapshot_from(from_block)`.
A `root_to_height` cache collision (`.or_insert` keeps the LOWEST height; the idle
state root recurs for thousands of empty blocks) drove the cursor **backward** to
4716 — below the already-verified frontier (~15615) — creating a ~47000-block
re-verification gap. `debug_executionWitness` is **O(tip − block)** (reth reverts
the latest trie back across the whole range; `-96000` is reth's synthetic
`ReadTransactionTimeout`, default 300s), so old-block witnesses time out and the
prover loops forever. Settlement was unaffected (composer self-signs the mock
proof; the prover is an off-critical-path verifier) so it was silent for ~10h.

## Core idea

The composer **already owns both halves of the frontier** with no new persistent
state:
- **POSTED**: it assembles every deferred window (`composer.rs:1508`, `1653`) and
  knows each Sync slot's `posted+1 .. sync_height` span.
- **VERIFIED**: every cryptographically-verified attestation lands in the
  `ProofStore` (`proof_sink.rs:127-139`, AFTER `verify_attestation` ecrecover).

So invert the protocol: the **composer dictates** `next_unverified` (the lowest
posted window still unattested) and the **prover becomes a stateless re-execution
worker**. This deletes the prover cursor, `root_to_height`, the re-anchor
machinery, and any re-verification of already-attested windows — by construction.

### Why this kills the 4716 class (both reviewers confirm)
The frontier now lives composer-side as `verified_frontier`, seeded from
`l1_head.cursor()` (`composer.rs:1955`) which is **L1-confirmed and structurally
monotone** (the deriver only advances it forward on L1 confirmation). It is keyed
by **content** (`publicInputsHash`) and L1-confirmation, NEVER by a re-executed-root
lookup, so it **cannot be driven backward** by a root collision. The corruptible
in-memory prover cursor that caused the spiral simply ceases to exist.

## Independence (a lying/buggy composer cannot forge a verified state)

The dictated range is a **HINT**; verification is **objective re-execution**. Three
binds survive **untouched**:
1. `validate_window` (`proverd main.rs:251-370`) runs the same guest-reth stateless
   re-execution and asserts every re-derived block hash == the composer-claimed
   `event.block_hash` (307-316).
2. `verify_settlement_public_inputs` (387-431) recomputes `publicInputsHash` byte
   for byte from the **authoritative `abi_calldata`**, never from
   `VerifyRange.public_inputs_hash` (that is cross-check + log only). The prover
   signs ONLY its own recomputed hash.
3. OD-5 `verify_settlement_chain` (836-840) binds `first.currentState` to the
   prover's OWN re-executed `parent_state_root` at `from-1`.

For the composer to advance `verified_frontier` it must obtain a signature, by the
registered attester key it does not hold, over a hash the prover independently
recomputed. Three layers, none forgeable by range-dictation. **The inversion adds
zero new trust.** Precondition: the `ProofSink` attester address MUST be provably
the on-chain registered attester, else a ProofSink-"verified" attestation could be
L1-rejected (frontier ≠ L1-settleable). [review-7]

## Protocol (additive wire change)

`control.proto` additions (no field renumber, no existing message changed):
- `message VerifyRange { from_block(1); to_block(2); rollup_id(3);
  claimed_current_state(4, 32B, HINT/cross-check); public_inputs_hash(5, 32B,
  cross-check only) }`
- `service ProverDispatch { rpc Dispatch(DispatchRequest) returns (stream VerifyRange) }`
- `message DispatchRequest { prover_epoch(1, log-correlation only) }`
- `ControlEvent`, `PostBatch`, `SlotProof`, `SubscribeRequest` are **byte-identical**.
  The prover still `Subscribe`s `ControlFeed` per directive to stream the block
  events (lower-diff than a single combined stream; see open question).

**A `VerifyRange` is ALWAYS a WHOLE posted batch** (`posted+1 → sync_height`).
Never an anchor-less sub-range. [review-3 — see Witness horizon]

## Composer driver

New shared state, one `Arc<Mutex<PostedWindows>>` wired where `proof_store` is today:
- `PostedWindow { from_block, to_block, rollup_id, public_inputs_hash,
  claimed_current_state, attested, fast_forwarded }`
- `PostedWindows { BTreeMap<sync_height, PostedWindow>, verified_frontier, highest_posted }`

- **POSTED fill**: in both Deferred arms (`composer.rs:1508`, `1653`), next to
  `optimistic.begin`, insert a `PostedWindow` (`from_block = posted+1` threaded out
  of `prepare_post_batch_raw`, `to_block = sync_height`, `attested=false`); bump
  `highest_posted`.
- **VERIFIED fill**: extend `ProofSinkSvc::verify_and_store` (`proof_sink.rs:127`) —
  AFTER `verify_attestation` returns true, find the `PostedWindow` matching the
  `publicInputsHash`, set `attested=true`, recompute `verified_frontier` (highest
  contiguous attested `to_block`). **The frontier advances EXCLUSIVELY via this
  content-keyed insert — never via any height the composer asserts.** [review-4]
- **Dispatch loop** (`ProverDispatch::dispatch`, new `prover_dispatch.rs`): read
  `next_unverified`; send a `VerifyRange`; await a `watch` (pinged by
  `verify_and_store`) until `verified_frontier >= to_block`; loop. Park on the watch
  when `next_unverified` is absent.

## Stateless prover worker

DELETE (or branch-guard behind `EEZ_COMPOSER_DRIVEN`): `last_accepted` (1180) +
`from_block` (1216); `root_to_height` (1197) + fill (1459-1469);
`settling_cursor_move`/`CursorMove` (1011-1032) + call (1860-1915);
`reanchor_move`/`ReanchorMove`/`MAX_CONSECUTIVE_RETREATS` (1039-1075, 1187) + the
`AnchorMismatch` self-heal (1707-1795); the cross-chunk telescope
(`batch_anchor_root`, 1251-1265, 1481-1502) — **a `VerifyRange` is one whole batch
= one chunk**, so OD-5's anchor is simply this window's re-executed
`parent_state_root` (no cross-chunk threading). `classify_settlement_chain` loses
its `AnchorMismatch` verdict.

KEEP UNTOUCHED (the soundness core): `validate_window`,
`verify_settlement_public_inputs`, `verify_settlement_chain` (anchor bind),
`multi_inbound_outcome_gate`, `verify_outbound_authorized`, sign+submit.

NEW loop: connect `ProverDispatch` → receive `VerifyRange{from,to,...}` →
`Subscribe(from_block=from)` → accumulate events until `block_number==to` (the
settling block; its composition MUST be present — else **refuse, request next, the
frontier physically CANNOT advance** since no attestation reaches the ProofStore)
[review-2] → if the ring evicted the range, `backfill_block` reconstructs from the
L2 archive (now the PRIMARY fetch for a behind window) → run the unchanged
soundness gates → sign `attest_hash` (the recomputed hash) → submit `SlotProof`.

**from_block self-correcting** [review-5]: the directive `from_block` is a hint. On
an OD-5 anchor mismatch (re-executed parent of `from` ≠ `first.currentState`), the
prover does a **single, bounded, stateless re-request** (ask the composer to
re-dictate `from_block`, recomputed from `l1_head.cursor()`), capped at 1 re-dictate
to avoid a flap — preserving liveness for an honest off-by-one WITHOUT
reintroducing prover-side persistent retreat. A second mismatch ⇒ HARD reject.

## Witness horizon (the O(tip − block) reality)

Driving per-batch keeps witnesses **near the tip in STEADY STATE only**. The
inversion does NOT make old-block witnesses fast — `debug_executionWitness` cost is
a reth property independent of who picks the range. [review-2, both lenses]

- **Steady state**: `next_unverified` trails the tip by O(1) batches → near-tip
  witness → fast. No gap forms. (This is the regime the inversion guarantees.)
- **Witness-size of a single batch**: solved by **posting NARROWER batches** (more
  frequent Sync slots), NOT by sub-splitting a posted batch into anchor-less pieces
  (that breaks OD-5 — a non-settling sub-window binds to nothing). [review-3]
- **Deep-gap RECOVERY** (the current 4716-vs-51000 state): the oldest-unverified
  `from_block` is far below tip → its witness STILL times out. **No design makes
  `debug_executionWitness(4716)@tip51000` cheap.** Recovery therefore REQUIRES the
  bounded **fast-forward**.

## Parallelization (per-block stateless re-execution)

`native-validate` validates each block STATELESSLY (`validate_block_stateless`
→ `stateless_validation_recovered_with_trie::<SparseState>`): each block
re-executes against its OWN witness (the `SparseState` it touches), independent
of every other block. `validate_one(blk) -> (parent_root, post_root, hash)` has
no cross-block state dependency, so the per-block re-execution is embarrassingly
parallel; only the cheap window-chain check is sequential.

    WINDOW [from..to]   (each block = its own witness → stateless)

      ┌─ blk N    ─► validate_one(N)    ─┐
      ├─ blk N+1  ─► validate_one(N+1)  ─┤  PARALLEL (rayon, one per core)
      ├─ blk N+2  ─► validate_one(N+2)  ─┤  each stateless vs its own witness
      └─ …        ─► …                  ─┘  → (parent_root, post_root, hash)
                          │
                          ▼
      chain check (SEQUENTIAL, O(n), cheap):
        post_root[k] == parent_root[k+1] ∀k  +  block_hash chain
        +  OD-5 anchor (parent of `from`)  +  telescope to claimed newState

Implement as `par_iter` (rayon) in native-validate's `--dir` window mode (our
zisk-eth-client fork). Soundness is UNCHANGED — each block's verification is
identical; only the execution ORDER changes, and the chain is asserted the same
way afterward.

CAVEAT — execution parallelizes, the WITNESS FETCH is the catch-up wall.
`validate_one` is CPU-bound (parallel across cores), but the deep-gap bottleneck
is `debug_executionWitness` (O(tip−block) on the L2 reth), NOT execution.
Parallelizing the FETCH for old blocks is risky — concurrent long MDBX read-txs
block the L2 reth's writer (the degradation observed with two stuck provers). So:
parallelize the EXECUTION (clear win for steady-state and wide windows); BOUND
the witness-fetch concurrency; for a deep gap use fast-forward, not parallelism.

## Fall-behind & fast-forward (recovery)

The composer detects lag itself (`verified_frontier < highest_posted`; the existing
deferred-post 30s timeout, `composer.rs:1784`).

- **FEED-THE-GAP** (steady-state / small gap): dictate `next_unverified` in posted
  order; the prover catches up window by window. Each is a coverage feed of a real
  posted batch, NEVER a re-verification (an attested window is removed from
  `next_unverified` by construction).
- **BOUNDED FAST-FORWARD** (when `highest_posted − verified_frontier > max_lag`):
  the composer marks those `PostedWindow`s `fast_forwarded` (≠ `attested`) and
  advances `verified_frontier` past them. **This is a COVERAGE GAP (those windows
  are never proven), NOT a re-verification.** Per-mode safety [review-1,2]:
  - *deferred-post mode*: safe because a non-attested window **never settled on L1**
    (the L1 post fires only after the attestation drains, `composer.rs:1784-1806`),
    so the L1 cursor never advanced past it.
  - *self-sign mode (current production)*: a fast-forwarded window MAY be settled on
    L1 (L1 advances on the mock self-sign without the prover). Still safe because the
    prover is **non-binding / off-critical-path** — skipping its attestation forfeits
    verification COVERAGE but cannot corrupt the L1 root.
- **CORRECTION vs the raw design**: to actually unstick the EXISTING deep gap,
  fast-forward must be the **recovery default / auto-engage above `max_lag`**, not
  opt-in. [both reviewers]

`verified_frontier` is **load-bearing ONLY in deferred-post mode**. In self-sign
mode `verify_and_store` never flips `attested` (no store) → it stays at its
cursor-seed and the driver is a pure observer feed. [review-5]

## Migration (phased, both modes preserved, one flag)

- **Phase 0** — proto additions, regenerate. No behavior change; old prover ignores
  `ProverDispatch`, still self-picks `from_block`.
- **Phase 1** — composer ledger, dark: add `PostedWindows` + fill in both Deferred
  arms + thread into `ProofSinkSvc`. Pure observability (assert `next_unverified`
  tracks deferred timeouts). No prover change.
- **Phase 2** — driver + dispatch service, spawned only when `proof_store` present,
  behind `EEZ_COMPOSER_DRIVEN` (default off → `Unimplemented` → provers self-drive:
  full backward compat).
- **Phase 3** — driven prover path behind the flag; the cursor code is
  branch-guarded so one binary serves both. Remove dead cursor code only after the
  driven path is validated in a deploy.

Backward-compat invariants: self-sign mode (`proof_store None`) unchanged, driver
never spawns. `SlotProof`/`ControlEvent`/`PostBatch` wire unchanged → a mixed fleet
interoperates.

## Edge cases (all sound; see review)

- **Prover restart**: stateless → reconnect → composer re-dictates `next_unverified`
  → re-fetch + re-attest. Kills the 4716 class. (Strictly better than today, where a
  restart resets `last_accepted=0` and replays `[1..tip]` — itself a spiral.)
- **Crash mid-verify**: no attestation recorded → `attested` stays false → re-dictate
  same window → idempotent (signing the same hash twice is a no-op).
- **Two provers**: each its own stream; first valid attestation flips `attested`;
  others observe the watch. *Fix*: GC/TTL the ProofStore so a late second attestation
  re-inserted after `spawn_deferred_post` drained doesn't leak. [review-6]
- **L1 reorg**: `l1_head.cursor()` retreats → composer re-posts as NEW windows with
  NEW hashes; `verified_frontier` sits at the un-reorged prefix. No prover-side
  retreat needed (it was the reanchor machinery's job, now the composer's via posted).
- **Composer restart**: `verified_frontier := l1_head.cursor()` — sound by
  **transitivity through L1** (deferred mode: a batch below cursor passed
  `ECDSAProofSystem.verify`, which requires the attester signature, so it WAS
  attested). The in-flight set above cursor reorgs out via `take_failed_for_recovery`
  and re-dictates. Only meaningful in deferred mode. [review-1,2]

## Change points

- `crates/eez-control-rpc/proto/control.proto`: add `VerifyRange`, `DispatchRequest`,
  `ProverDispatch`. Keep everything else byte-identical. Regenerate.
- `crates/eez-composer/src/proof_sink.rs`: add `PostedWindow`/`PostedWindows`; extend
  `verify_and_store` (flip `attested`, recompute `verified_frontier`); add a watch.
- `crates/eez-composer/src/composer.rs`: `posted_windows` field + setter; fill in both
  Deferred arms (`1508`, `1653`); thread `posted` (`l1_head.cursor()`, `1949-1955`).
- `crates/eez-composer/src/prover_dispatch.rs` (new): the `ProverDispatch` service +
  per-prover dispatch loop.
- `crates/eez-node/src/main.rs`: build the `PostedWindows` Arc next to `proof_store`;
  `set_posted_windows`; `add_service(ProverDispatchServer)` gated on `proof_store` +
  `EEZ_COMPOSER_DRIVEN`.
- `crates/eez-proverd/src/main.rs`: add the `ProverDispatch` client + `VerifyRange`
  loop; delete/branch-guard the cursor/reanchor/telescope code; `backfill_block`
  becomes primary; bounded from_block re-request.

## Open questions

- **Single combined stream** (directive + block events in one `ProverDispatch`
  stream) vs the two-stream form (reuse `ControlFeed` replay). Two-stream is
  lower-diff; single-stream is cleaner but duplicates the ring/replay logic.
- **Multi-rollup**: key `PostedWindows` by `(rollup_id, sync_height)` + per-rollup
  `verified_frontier` + per-rollup dispatch.
- **Sub-window splitting for deep-gap recovery WITHOUT fast-forward**: only if we
  must preserve every-window-proven during recovery — requires the composer to
  expose each block's post-exec root (it has them in the witness feed) so a
  sub-range carries its own re-derivable `claimed_current_state`. Otherwise the
  deep-gap escape is fast-forward. [review-3, elevate from open-q to a decision]
