# STATE vs PLAN — the anchor

**Purpose:** one place that pins where the port stands against the authoritative plan, so we
don't drift. **Updated:** 2026-06-20 · **Branch:** `feat/port-based-crosschain-core` (44 commits
ahead of `main`, not merged — Edu merges).

**Authoritative plan:** `/home/ubuntu/based-rollup/docs/migrate-to-eez-rollup0-plan.md` (v2, LOCKED).
**Companion docs:** `prover-chain-and-chiado-deploy.md` (prover + Chiado), `crosschain-upgrade-to-based.md`
(crosschain completeness + the deferred deliver-as-failed blueprint).

---

## 1. Plan phases → status

| Plan phase | Status | Evidence (commits) |
|---|---|---|
| −1 pre-flight / naming pre-step | ✅ | port commits |
| **A** — ABI flip #27→#28 + deriver decode-canary + negative assertion | ✅ | `66a26c6` (submodule), `46aa93b` (selectors), `99c412c` (canary) |
| **B** — core swap (protocol+evm) | ✅ | `9a6aaf7`, `46aa93b`, `dcd74e5`, `276ad70` |
| **B** — INBOUND relocation (port `inbound.rs`) | ⛔ **DEVIATION** | NOT ported — see §2.1 |
| **C** ★ system_tx (TxLegacy→TxEip1559 + byte-identity vector) | ⚠️ **DEVIATION** | done differently — see §2.2 |
| **D** — mid-layer re-point + reth reconciliation | ✅ | port commits + `f9f83d9` (auth-port prod fix) |
| **E** — infra + genesis + Mock→real PS + registry-id assertion (C1) | ✅ | `c8f6fd6` (C1), `dbb8ef1` (genesis re-bake), `b58cebe` (real PS) |
| **P1** — in-process witness fold | ✅ | `7c91f0a` (0a), `dc569f9` (P1) |
| **P2** — control-rpc (the prover feed/sink) | ✅ | `3e6218c`, `9f7ff39`, `b4ccd3e` |
| **P3** — real prover binary + 6-axis gate + native-validate | ✅ | `fd929d7`…`9b6f3f3`, `d71e512` (attest) |
| **P4** — composer seam (replace sync `prove()` → deferred-post via ProofSink) | ✅ | `27a448d`, `a05087a`, `afcab2c`, **`3a7dca3`** (deferred dispatch) + `c1de5cc` (review fix) |
| **Chiado deploy** (replan: real PS on Chiado, the genesis/PS swap from Phase E) | ✅ live | `b58cebe`, `ef00970`; Phase 4 validated on-chain (`5eb30d6`) |
| **Gate** — validator-mode tamper-refusal (C2) | ⚠️ **PARTIAL** | `c792785` — see §2.3 |
| **Slice (outbound-only)** — L2→L1 settling on real L1, attested by the real prover | ✅ live | Chiado Phase 4 (empty/minimal slots) |
| **INBOUND (last)** | ⏳ **WE ARE HERE** | see §3 |

**Bottom line: Phases A–E + P1–P4 + the outbound slice + the Chiado real-PS deployment are DONE
and validated live on-chain.** We are at the plan's final stage, **INBOUND (last)**.

---

## 2. Deviations from the plan (the things to be honest about)

### 2.1 Phase B inbound.rs port → deferred (poison-evict kept)
The plan: *"Phase B MUST ALSO port `composer-lib/inbound.rs`"* (or inbound regresses). Execution:
eez0 kept its OWN native inbound (ingress → held_pool → compose → `build_inbound_system_txs` →
deliver) and did NOT port based's `orchestrate_inbound_l1`. **eez0's inbound WORKS for the success
path; reverting inbounds are dropped (poison-evict), the user resubmits.** The plan's "deliver-as-
failed" is what's missing. **Status: formally DEFERRED (Edu, 2026-06-20)** — blueprint in
`crosschain-upgrade-to-based.md` Appendix A. *This is the "INBOUND (last)" unit, not an off-plan
detour.*

### 2.2 Phase C ★ (the plan's single-highest-risk item) — done differently
The plan: switch `TxLegacy→TxEip1559` to match based + build a `composer-emit == deriver-rebuild`
byte-identity vector with **base_fee varying per block**. Execution: **kept `TxLegacy`** (a single
shared `build_inbound_system_txs` fn on both composer + deriver) and deemed the vector "moot"
(identity holds by construction; TxLegacy has no `base_fee` → the `max_fee=base_fee*2` asymmetry
trap the plan feared **doesn't apply**). **Defensible — arguably neutralizes the risk by
construction — but the plan's mandated gate (the varying-base_fee vector) was not built.**
**✅ ACCEPTED (Edu, 2026-06-20).** Residual risk: composer-emit==deriver-rebuild byte-identity is
guaranteed only while both call the shared fn with the SAME inputs. **✅ Follow-up DONE (`c6b397a`):**
`eez-evm/system_tx.rs` now has a byte-identity vector — `build_inbound_system_txs(entries)` ==
`build_inbound_system_txs(decode_postbatch(encode_postbatch(batch)).entries)`, byte-for-byte (+ a
non-vacuous nonce-varies check). Covers the encode/decode round-trip preservation. NOT covered
(separate, no live inbound fixture): the composer's `target.batch.entries` == the deriver's
`source.batch` entries agreement — a composition concern, not the shared-fn byte-identity.

### 2.3 C2 tamper-refusal gate — partial (different axis than specified)
The plan: validator-mode E2E refusing a tampered **interior root / reverted system tx / mismatched
parent root**. Execution: `scripts/soundness-tamper-refusal.sh` (`c792785`) proves the **re-execution
axis** (a tampered witness → `native-validate` rejects → prover refuses). **The interior/system/parent
tamper cases the plan named are NOT specifically built.** **✅ ACCEPTED (Edu, 2026-06-20):** the
re-exec axis (witness faithfulness) is the most fundamental and is covered; the settlement gates
(interior/#10/parent) EXIST + are unit-tested. **Follow-up (noted, not now):** a tamper-refusal E2E
for the interior/system/parent gates.

---

## 3. Where we are + what remains (the INBOUND-last unit)

The plan's inbound unit = **(a)** the prover inbound outcome gate (X==Y, the soundness close) +
**(b)** deliver-as-failed + **(c)** `settlement_tracker`. Mapping to current reality:

| Item | Plan | Status |
|---|---|---|
| Prover inbound outcome gate (X==Y) + multiplicity | INBOUND-last, the soundness close | ✅ **DONE + AUDITED** (`1ac0132` + `64ff7c1`) — ported to `eez-proverd`, wired into the settling-window gates, unit-tested, C2 ship-gate green. **Auditor PASS** after finding+fixing **C1** (a verbatim-port bug: the deferred inbound entry was read at `entries[0]`, but eez0 prepends a leading-immediate entry, so it's at `entries[1]` — fixed to locate it by H; fail-closed throughout). |
| deliver-as-failed (`success=false`) | Phase B inbound.rs | ⛔ **DEFERRED** (Edu) — blueprint preserved |
| `settlement_tracker` | composer-lib port (robustness) | ⏳ not done (robustness, not soundness) |
| L1-originated inbound (`l1_interceptor`) | NOT in the plan's first inbound unit | 🅿️ **out of scope** unless product needs L1-originated calls |

---

## 4. Corrected understandings (anchored so we don't re-trip)

These cost real back-and-forth; pin them:
- **eez0 has a NATIVE, working inbound** (poison-evict for failures). It was NEVER "non-functional."
- **Core crates (`eez-protocol`/`eez-evm`) are at PARITY or AHEAD of based modular** — not "old."
  eez0 even FIXED `ROLLUPS_MAPPING_SLOT` (eez0=2 correct; based=1 latent bug) and added the deriver
  decode canaries. The "old" part is the composer-orchestration *completeness* (deliver-as-failed,
  settlement_tracker), all deferred/optional.
- **`system_tx.rs:110` `success:true` is SAFE today** — guarded by poison-evict (only successful
  inbounds reach it). Not a live bug.
- **Method:** verify in-tree / against the `.sol`; do not trust comparison-framed agent recon
  (it twice concluded "missing/bug" where it was an eez0-deliberate variant).

---

## 5. Decisions (resolved — Edu, 2026-06-20)
1. **All 3 deviations ACCEPTED** (§2.1 inbound deferred, §2.2 Phase-C as-is, §2.3 C2 re-exec axis).
   Documented; follow-ups noted (Phase-C end-to-end byte-identity vector; interior/system tamper E2E).
2. **✅ DONE: the prover inbound outcome gate (X==Y) + multiplicity** (`1ac0132`) — the plan's
   "INBOUND (last)" soundness close. Gates that the sealed L2 inbound delivery's `returnData`/hash
   match the L1-settled entry (catches a composer delivering X on L2 but settling Y on L1); fail-
   closed on >1 inbound. Read-only over the feed; the working composer is untouched; C2 ship-gate
   still green. **AUDITED — PASS** (`64ff7c1`): the auditor found C1 (deferred entry read at
   `entries[0]` vs eez0's leading-immediate prepend → it's at `entries[1]`; fixed to find by H) and
   re-confirmed sound + fail-closed. The failure path is complete but dormant (eez0 poison-evicts
   reverts) — it activates when deliver-as-failed lands.
3. **L1-originated inbound** — parked (not this cycle unless product needs it).
