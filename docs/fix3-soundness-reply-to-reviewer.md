# Reply to the Reviewer — Fix 3 Soundness (Round 2)

Date: 2026-06-27

This is a direct reply to your evaluation (`docs/fix3-soundness-review.md`). My
full code-verified analysis is in `docs/fix3-soundness-response.md`; this note is
the short version addressed to you: where we agree, the two places your framing
needs correcting, the one catch of yours worth folding in exactly, and the
questions I need you to sign off on so we can close this.

Every citation below was re-checked against the source.

---

## Where we agree

- **The pre-fix behavior was genuinely wrong.** The settling sidecar was chosen by
  `find_map`'s first hit over the replayed range (`main.rs:1734-1736`), so an older
  interior sidecar could be validated as the directive's settlement and the
  frontier stalled. Height-keying the selection is the right minimal fix.
- **Fix 3 opens no state-forgery hole.** It only nulls `event.composition`; every
  block is still re-executed (`main.rs:1624`, `1662-1664`, `265-282`) and the
  target's HARD gates run. Confirmed.
- **Keep Fix 3, harden before closing.** Agreed on the verdict and on the
  three-item hardening direction.
- **Your guard-key catch is correct and useful** — see "the one catch worth
  folding in" below. It improves on the originally-proposed guard.

So we are not far apart. The corrections below do not change your conclusion; they
make "sound for today" *stronger* than your draft argues, and they keep a future
reader from drawing a wrong inference.

---

## Correction 1 — the inbound-interior threat is moot **today**, not an open gap

You frame "a rich, canonical, non-target sidecar inside a later directive range"
as a live risk the strip fails to defend against. For the **inbound** case — the
one your "naive guard" section is built around — that scenario is structurally
impossible under the current composer, and the code says so directly:

- An inbound delivery is a `HeldTx{Inbound}` that flows into `survivors`, and
  **every** compose arm calls `optimistic.begin(sync_height, …, survivors)` with
  the full vec (Ready `composer.rs:1636-1641`, Deferred `1691-1696`).
- So `FailedBatch.txs` is non-empty whenever any inbound delivery is present, which
  makes `recover_failed_batch`'s `!failed.txs.is_empty()` guard
  (`composer.rs:990`) TRUE → the block is **reorged out**.
- The empty/minimal path uses `Vec::new()` survivors and carries no inbound
  delivery by construction.

⟹ **A failed inbound-bearing block can never become a non-target interior
sidecar.** The "interior settling blocks are always minimal" invariant holds, and
it is enforced by code, not merely assumed. The right characterization is: *no
present gap; the shape-check is defense-in-depth against a future composer
change*, not a patch for something reachable today. Your own lines 137-141
("acceptable for the present bug … not robust against future changes") are the
accurate reading — they just undersell how firmly the invariant currently holds.

---

## Correction 2 — "signs regardless" needs the two-hash split, or it misleads

Your "Separate Concern" is a real issue and correctly separated from soundness —
but as written a reader can conclude the prover signs an unconstrained hash. It
does not. There are **two** hash checks with **opposite** failure modes:

- **HARD** — `verify_settlement_public_inputs` (`main.rs:444-488`, called at
  `1750`) `bail!`s (`484-486`) when the recomputed hash disagrees with the
  composer's claim baked into the **on-wire postBatch calldata**
  (`pb.public_inputs_hash`).
- **SOFT** — the check at `main.rs:2085` compares only against the out-of-band
  directive **hint** (`vr.public_inputs_hash`), which the protocol explicitly
  designates a hint/cross-check (`control.proto:178-189`). This is the one that
  logs `error!` and signs anyway (`main.rs:2083-2092`).

So the prover never signs a hash that contradicts the authoritative calldata; it
only "signs regardless" of the *ledger hint*. ⟹ The residual hazard is
**accounting/liveness** — a proof resolving a content-identical *sibling* window
(reachable via the N=0 sentinel that omits a per-L1-block nonce,
`public_inputs.rs:313-314`; the empty-heartbeat collision is documented in-code at
`posted_windows.rs:204-214`) — **never** a bad transition settling. OD-5
re-execution + the HARD calldata cross-check + on-chain `StateRootMismatch` bind
every signed hash to a re-executed transition. Please state this split explicitly;
your recommendation (refuse on hint mismatch) is right, but for the liveness
reason, not a soundness one.

---

## The one catch worth folding in exactly

You are right that a guard keyed on `l2ToL1Calls.is_empty()` is wrong, and this is
the most useful correction in your review. The on-chain inbound entry is lean:
`build_l1_inbound_entry` sets `l2ToL1Calls: Vec::new()` (empty) but
`proxyEntryHash != 0` (`entries/mod.rs:606-616`). The repo's canonical
discriminator is explicit: *"discriminate on `proxyEntryHash`, NOT
`l2ToL1Calls`-emptiness"* (`deriver.rs:1204-1218`). **The correct key is
`proxyEntryHash`.**

One sharpening back at you: your wording ("inbound entries may have empty
`l2ToL1Calls`") is true only for the **on-chain `entries[]`**. There is a second,
**populated** inbound representation — the DA sidecar (`build_l1_inbound_sidecar`,
`entries/mod.rs:662+`) with a **non-empty** `l2ToL1Calls[0]` that the deriver
requires and filters on (`deriver.rs:1217`). Same logical event, opposite
emptiness. A guard must decode the on-chain `entries[]` (which is what a
PostBatch-decoding guard sees), where your claim holds — but the two
representations should be named so nobody keys the guard on the wrong one.

---

## Converged hardening spec

If you concur, this is the exact thing to build — both your catch and the two
corrections folded in:

1. **Fail-closed minimal-shape check before stripping** (`driven_effective_settlement`,
   `main.rs:1048`), keyed on `proxyEntryHash`, reusing the 3-way
   `(proxyEntryHash, l2ToL1Calls)` classification at `decode_batch.rs:34-43`:
   - the only entry is the leading anchor (`proxyEntryHash == 0 &&
     l2ToL1Calls.is_empty()`);
   - **no** entry with `proxyEntryHash != 0` (inbound deferred);
   - **no** entry with `proxyEntryHash == 0 && !l2ToL1Calls.is_empty()` (outbound);
   - `callData` decodes to a `DecodedBatch` with `l2_entries` empty and the
     last-block `transactions` empty.
   - On any mismatch → `window_ok = false` (do not strip).
2. **Refuse to submit on directive-hint mismatch** (`main.rs:2083-2092`) — a
   liveness guard. The deeper cure is activating `blockNumber = N` so the public
   inputs carry a per-L1-block nonce (`public_inputs.rs:308-356`) and distinct
   windows can never share a ledger key.
3. **The four integration tests** you listed (stale-minimal, stale-rich-rejected,
   missing-target-refuse, hash-mismatch-no-submit). Current unit tests cover
   selection only (`main.rs:2281-2296`).

None of this is urgent: there is no live soundness hole. It converts the prover
from *relying on composer invariants enforced in another crate* to *locally
self-defending*.

---

## Questions for you (to close this out)

1. Do you agree the inbound-interior case is **moot today** (reorged via
   `composer.rs:990`), i.e. the shape-check is future-proofing, not a present-hole
   fix? If you see a path where an inbound or outbound delivery lands with
   `failed.txs` empty, name it — that would change the urgency.
2. Do you accept the two-hash split (HARD calldata cross-check vs SOFT hint), i.e.
   that the "signs regardless" risk is liveness/accounting, not soundness?
3. Any objection to keying the guard on `proxyEntryHash` + the 3-way classifier at
   `decode_batch.rs:34-43`, rather than re-deriving a shape predicate?

If we agree on these three, the hardening spec above is final and I will implement
it.
