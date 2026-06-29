# Response to the Fix 3 Soundness Evaluation

Date: 2026-06-27

This document responds to `docs/fix3-soundness-review.md` (the independent
evaluation of Fix 3 — the driven-prover non-target `PostBatch` sidecar strip in
`crates/eez-proverd/src/main.rs`, `driven_effective_settlement` at `main.rs:1040`).

Every load-bearing claim in the evaluation was re-checked against the actual
source (not against either document's prose). The citations below are the result
of that pass.

---

## TL;DR

- **The evaluation is accurate and trustworthy. No claim in it is refuted.** Its
  conclusion holds: keep Fix 3; it is sound today against state forgery; harden
  before declaring the area closed.
- Two of its points need a correction or a sharpening, **neither of which changes
  its conclusion** — both actually make "sound for today" *more* defensible:
  1. **The "naive guard correction" is right about the field but moot about the
     threat.** A guard keyed on `l2ToL1Calls.is_empty()` is indeed wrong — but the
     scenario it guards against (an inbound-bearing block surviving as a
     non-target interior sidecar) **cannot occur under the current composer**. The
     correct discriminator is `proxyEntryHash != 0`, not `l2ToL1Calls`.
  2. **The hash-mismatch concern is real but under-disambiguated.** There are
     *two* hash checks with opposite failure modes. The HARD one binds every
     signed hash to the authoritative calldata; the "sign-regardless" path only
     ignores the out-of-band directive *hint*. The residual risk is
     accounting/liveness, never a bad transition settling.

---

## 1. Where the evaluation is correct (confirmed against code)

All of the following were verified and stand:

- **The bug.** Directive `from_block = posted+1`, `to_block = settling Sync block`
  (`posted_windows.rs:28-32`, `prover_dispatch.rs:55-62`, `main.rs:1356-1361`). A
  replayed range can carry multiple historical `composition.post_batch` sidecars
  (`control_feed.rs:131-141` replays all `block_number >= from_block`); pre-fix,
  the sidecar was selected by `find_map`'s **first** hit (`main.rs:1734-1736`), so
  an older interior sidecar could be validated as the directive's settlement and
  the frontier stalled.
- **The fix changes selection only.** `driven_effective_settlement`
  (`main.rs:1040-1058`) only nulls `event.composition`; `window.push(event)`
  (`main.rs:1624`) still pushes the block bytes and `validate_window`
  (`main.rs:1662-1664`) re-executes **every** block in the range. No execution is
  skipped.
- **No state-forgery hole.** At the target, all gates run: HARD `publicInputsHash`
  cross-check (`main.rs:444-488`, called at `1750`), settlement-delta chain
  (`1785`), OD-5 anchor (`893`) — and the anchor is bound to a **re-executed** root
  (`vw.parent_state_root`, `main.rs:1714`), not a composer hint. Interior blocks
  whose sidecar is stripped are still fully re-executed and bound to their claimed
  hash (`per_block_roots` `main.rs:265-282`; interior-boundary gate `934-985`). So
  the worst possible failure is *semantic* (a settlement check skipped at the
  height it appeared), never state forgery.
- **The composer invariants the safety argument rests on.** One-in-flight gate
  emits empty Sync blocks while a post is unresolved (`composer.rs:793-826`);
  `recover_failed_batch` reorgs only when `!failed.txs.is_empty()`
  (`composer.rs:990`); settled rich blocks sit below the next directive cursor
  (`from_block=settled+1`, `composer.rs:411-419`); same-anchor **unresolved**
  prefixes are superseded by the widest window (`posted_windows.rs:182-188`).
- **The cross-chain gates are real and HARD.** Inbound is a full N:N bijection
  (`multi_inbound_outcome_gate`, `main.rs:635-743`, HARD at `1847/1853`); outbound
  (A3) extracts signed user txs from the window's last-block RLP and binds via the
  shared `verify_outbound_authorized` (`main.rs:1858-1926` → `outbound_gate.rs:134-228`,
  HARD at `1905`, attest blocked at `1935`). A3/A4 share one module with a
  drift-guard test (no prover/deriver drift).

The evaluation's framing — "acceptable for the present bug … not robust against
future changes" — is the accurate reading. Our only disagreement is that it
**under-sells how strongly the invariant currently holds** (next section).

---

## 2. Correction 1 — the "naive guard correction" is right about the field, moot about the threat

The evaluation states a guard keyed on `l2ToL1Calls.is_empty()` is insufficient.
**Correct, and it fixes a real footgun in the originally-proposed guard.** The
on-chain inbound entry is built lean: `build_l1_inbound_entry` sets
`l2ToL1Calls: Vec::new()` (empty) but `proxyEntryHash != 0`
(`entries/mod.rs:606-616`). The repo's own canonical discriminator agrees —
*"discriminate on `proxyEntryHash`, NOT `l2ToL1Calls`-emptiness"*
(`deriver.rs:1204-1218`). **The correct key is `proxyEntryHash != 0`.**

**But the threat that guard would cover cannot arise under the current composer.**
For it to bite, a *failed* inbound-bearing Sync block would have to stay canonical
and reappear as a non-target interior block in a later directive. It cannot:

- An inbound delivery is carried as a `HeldTx{Inbound}` that flows into
  `survivors`, and **every** compose arm calls `optimistic.begin(sync_height, …,
  survivors)` with the full vec (Ready arm `composer.rs:1636-1641`, Deferred arm
  `1691-1696`).
- So `FailedBatch.txs` (== survivors) is non-empty whenever any inbound delivery
  is present. `recover_failed_batch`'s `!failed.txs.is_empty()` guard
  (`composer.rs:990`) is therefore TRUE for any inbound-bearing block → it is
  **reorged out**.
- The empty/minimal path uses `Vec::new()` survivors and carries no inbound
  delivery by construction.

⟹ **A failed inbound-bearing block can never become a non-target interior
sidecar.** The original soundness argument (interior settling blocks are always
minimal) had **no hole**; the evaluation's correction refines the *guard design*,
it does not break the proof. The hardening is **defense-in-depth against a future
composer regression**, not a patch for a live gap.

**Precision gap to record (the evaluation's own imprecision):** there are *two*
inbound representations with **opposite** `l2ToL1Calls` emptiness — the lean
on-chain entry (`build_l1_inbound_entry`, empty) and the populated DA sidecar
(`build_l1_inbound_sidecar`, `entries/mod.rs:662+`, `l2ToL1Calls[0]` non-empty,
which the deriver *requires* and filters on, `deriver.rs:1217`). The evaluation's
"inbound entries may have empty `l2ToL1Calls`" is true **only for the on-chain
`entries[]`** — which is exactly what a PostBatch-decoding guard inspects, so the
claim holds where it matters, but it should name the representation.

---

## 3. Correction 2 — the hash-mismatch concern is real but under-disambiguated

The evaluation's three sub-claims are confirmed:

- On a recomputed-hash vs directive-hint mismatch the prover logs `error!` but
  still signs and submits the recomputed hash (`main.rs:2083-2092`).
- The composer ledger is content-keyed by `publicInputsHash`
  (`PostedWindow.public_inputs_hash` `posted_windows.rs:36`; `mark_attested`
  `199-225`; `proof_sink.rs:144-163`). Range fields do not participate in matching.
- Two distinct windows **can** share a hash because of the N=0 sentinel: the
  public inputs omit a per-L1-block nonce (`getTimestampAndBlockHash(0) = (0,0)`,
  `public_inputs.rs:313-314`); the documented empty-heartbeat collision is at
  `posted_windows.rs:204-214`.

**The disambiguation the evaluation does not make, and should:** there are **two**
hash checks with **opposite** failure modes.

- HARD: `verify_settlement_public_inputs` (`main.rs:444-488`, called at `1750`)
  `bail!`s (`484-486`) when the recomputed hash disagrees with the composer's
  claim baked into the **on-wire postBatch calldata** (`pb.public_inputs_hash`).
- SOFT: the check at `main.rs:2085` compares only against the out-of-band
  directive **hint** (`vr.public_inputs_hash`), which the protocol explicitly
  designates a hint/cross-check (`control.proto:178-189`).

So **the prover never signs a hash that contradicts the authoritative calldata**.
It only "signs regardless" when the composer's separate *ledger hint* disagrees.
⟹ The residual hazard is **accounting/liveness** (a proof resolving a
content-identical sibling window, or the frontier failing to advance), **not** a
state-soundness break — OD-5 re-execution + the HARD calldata cross-check +
on-chain `StateRootMismatch` still bind every signed hash to a re-executed
transition. A reader of the evaluation could mistakenly conclude the prover signs
an unconstrained hash; it does not.

---

## 4. Net position and corrected hardening

**Accept the evaluation. Keep Fix 3.** It is sound today against state forgery;
the pre-fix `find_map`-picks-first behavior was genuinely wrong; the height-keyed
strip is the right minimal correction and does not skip execution. None of the
hardening is urgent — there is no live soundness hole.

The hardening, with both corrections folded in:

1. **Fail-closed minimal-shape check before stripping a non-target sidecar**
   (`driven_effective_settlement`, `main.rs:1048`). Decode the sidecar's
   `PostBatch.abi_calldata` → `entries[]` + `callData` and require the minimal
   shape, keyed on **`proxyEntryHash`, NOT `l2ToL1Calls`**:
   - the only entry is the leading anchor (`proxyEntryHash == 0 &&
     l2ToL1Calls.is_empty()`);
   - **no** entry with `proxyEntryHash != 0` (rejects inbound deferred — the case
     an `l2ToL1Calls`-emptiness check would miss);
   - **no** entry with `proxyEntryHash == 0 && !l2ToL1Calls.is_empty()` (rejects
     outbound);
   - `callData` decodes to a `DecodedBatch` with `l2_entries` empty and the
     last-block `transactions` empty (no sidecar-derivation entries, no user txs).

   The 3-way `(proxyEntryHash, l2ToL1Calls)` classification already exists at
   `decode_batch.rs:34-43` — reuse it. On a non-minimal shape → `window_ok = false`
   (fail closed), do **not** strip.

2. **Refuse to submit on directive-hint mismatch** (`main.rs:2083-2092`): replace
   the `error!`-and-continue with skip + re-request — a fail-closed liveness guard
   against resolving a content-identical sibling window. The deeper cure is making
   the hash collision-free: activate `blockNumber = N` so
   `getTimestampAndBlockHash(N)` injects a per-L1-block nonce
   (`public_inputs.rs:308-356`) and distinct windows can never share a key.

3. **Add the four integration tests** the evaluation lists (stale-minimal,
   stale-rich-rejected, missing-target-refuse, hash-mismatch-no-submit). The
   current unit tests cover sidecar selection only (`PostBatch::default()`
   fixtures, `main.rs:2281-2296`), not the downstream gates.

Incidental cleanup noticed during verification (not related to Fix 3): the
`held_pool.rs:62-66` comment says outbound "is not yet wired" — outbound *is*
wired (`composer.rs:1274-1365`). Stale comment.

---

## Code reference table

| Subject | Location |
| --- | --- |
| Fix 3 strip helper | `eez-proverd/src/main.rs:1040-1058` (strip at `1054`) |
| Sidecar selection (pre-fix `find_map` first-hit) | `eez-proverd/src/main.rs:1734-1736` |
| Window re-executes every block | `eez-proverd/src/main.rs:1624`, `1662-1664`, `265-282` |
| HARD publicInputsHash cross-check (calldata) | `eez-proverd/src/main.rs:444-488` (bail `484-486`), called `1750` |
| SOFT directive-hint check (sign-regardless) | `eez-proverd/src/main.rs:2083-2092` (hint `vr.public_inputs_hash`) |
| OD-5 anchor vs re-executed root | `eez-proverd/src/main.rs:1714`, `893` |
| Interior-boundary gate | `eez-proverd/src/main.rs:934-985` |
| Inbound bijection gate (HARD) | `eez-proverd/src/main.rs:635-743`, `1847/1853` |
| Outbound gate A3 (HARD) | `eez-proverd/src/main.rs:1858-1926`, `1905`, `1935` → `eez-evm/src/outbound_gate.rs:134-228` |
| One-in-flight gate | `eez-composer/src/composer.rs:793-826` |
| Rich-block reorg on failure | `eez-composer/src/composer.rs:986-1010` (gate `990`) |
| Inbound `HeldTx` → survivors → `optimistic.begin` | `eez-composer/src/composer.rs:1636-1641`, `1691-1696` |
| Settled rich below next cursor | `eez-composer/src/composer.rs:411-419` |
| Ledger content-keying / supersede-unresolved | `eez-composer/src/posted_windows.rs:36`, `199-225`, `182-188` |
| Lean on-chain inbound entry (empty `l2ToL1Calls`, `proxyEntryHash != 0`) | `eez-evm/src/entries/mod.rs:595-634` |
| Populated DA inbound sidecar (non-empty `l2ToL1Calls`) | `eez-evm/src/entries/mod.rs:662+` |
| Canonical discriminator: `proxyEntryHash`, not `l2ToL1Calls` | `eez-deriver/src/deriver.rs:1204-1218` |
| 3-way entry classification | `decode_batch.rs:34-43` |
| N=0 sentinel (hash omits per-L1-block nonce) | `eez-evm/src/public_inputs.rs:308-356` (`313-314`) |
