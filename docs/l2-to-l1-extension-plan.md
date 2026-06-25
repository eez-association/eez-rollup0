# Bidirectional cross-chain for eez0 — entry topology + the L2→L1 extension

**Status:** IMPLEMENTING — A0 ✅, A1.1 ✅, A2.1 ✅, A2.2 ✅, **A2.3 ✅ (full composer-side outbound wiring: a–e)**; NEXT A2.4 (deriver) + P-1 (anvil E2E) · **2026-06-21** · **rev 15**
(3 adversarial reviews + 1 verification applied. Q7=state-mutating value-free N=1 + hard-reject value/reverting/
reentrant. Architecture: extend-eez0, ONE composer/per-composition entry, local source-sim, mixed batch, N=1 first
cut, L2 `loadExecutionTable` wired [A2(a0)]. **P-1 gates A2+; A0/A1 may start now.** Ready to implement on Edu's go.)

Goal: add **L2→L1** cross-chain CALLS to eez0 alongside L1→L2, with **both directions settling in the same
postBatch**, AND fix the **entry-point topology** so real wallets work — **extending, not breaking** the L1→L2 path.

> **Direction strategy (decided):** eez0 settles **whatever the L2 block contains** — an inbound delivery AND an
> outbound call can coexist in one block, so the single postBatch for that block carries **both** (mixed layout
> `[anchor | outbound immediates | inbound deferred]`). **The contract already TOLERATES this** (verified:
> `postAndVerifyBatch` drains the leading run of `proxyEntryHash==0` immediates `EEZ.sol:387-397`, then publishes
> the deferred remainder content-addressed `:672-676`). based's single-direction-per-slot (`merge_target_prefix`/
> `is_inbound`, `driver.rs:1226`) is a **reference choice, NOT a correctness requirement** — we do NOT adopt it just
> because based does. The mixed batch is **NEW R&D in the eez0 composer/deriver/prover** (not "reuse"), fully
> characterized in §1/§3/§5 below. (The "deposits+withdrawals coexist" precedent IS real but lives in
> based-rollup-OLD; we re-derive the mix correctly here rather than assume it.)

---

## 0. Entry topology (wallet ports) — CONFIRMED in-tree ✅

A wallet (Rabby/MetaMask) reads `chainId`/`nonce`/`gasPrice`/`balance` from the RPC it is connected to, then signs
and submits to that same RPC. So the **entry endpoint is per-NETWORK**, not one shared port. (Re-verified: eez0's
`:18688` answers `eth_chainId`/`eth_getTransactionCount` with **L2** context — `chiado-node.yml:83-84`; a real L1
wallet there would build a wrong tx. `devnet-test.sh:145,148` only works because it HAND-builds the L1 tx with
`cast mktx --chain-id $L1_CHAIN_ID --nonce <L1 nonce>`. based already ships two fronts: `l1_interceptor` (L1) +
`crosschain_rpc` (L2).)

- **`:18688` — eez-node L2 RPC** → the **L2 network**: serves **L2-pure + L2→L1** (both L2 txs, wallet-on-L2).
  Routed by the ingress classifier (`ingress.rs:112-124`; today an L2-proxy address or foreign chain-id → CrossChain).
- **`:18646` — `l1_interceptor` (NEW)** → the **L1 network**: serves **L1→L2**. A complete L1 RPC front:
  forwards every `eth_*` verbatim to the real L1 (`:18645`) so the wallet reads correct L1 nonce/chainId/gas, and
  intercepts only `eth_sendRawTransaction` to **detect + PUSH** the L1→L2 tx into the HeldPool. **Reduced role:
  detect + push — do NOT port based's `orchestrate_inbound_l1`** (based composes at ingress; eez0 composes at
  drain). Re-verified: every eez0 pool insertion is a raw `HeldTx`; an externally-pushed raw tx drains identically.

**Accumulation + emission (both directions, one batch):** both directions **accumulate** in the HeldPool AND are
**emitted together** in one postBatch (the mixed layout the contract tolerates). The one thing that stays
**per-direction** is the nonce-contiguity index and the admission gate (L1-context nonce for inbound vs L2-context
nonce for outbound — same EOA, different sequences; so contiguity is keyed `(sender, direction)`). So Edu's
"accumulate + settle together" holds end-to-end; only the nonce/admission bookkeeping is split by direction.

```
 Wallet@L1 ─▶ :18646 l1_interceptor ─ eth_* ─▶ :18645 (L1 real)        [admission: L1 nonce+balance]
                       └ sendRawTx ─ detect L1→L2 ─ push(dir=inbound) ┐
                                                                       │
 Wallet@L2 ─▶ :18688 eez-node ─ ingress ┬ no-proxy ─▶ reth mempool ─▶ L2 block      (C: L2-pure)
                                        └ to∈proxy-L2 ─ push(dir=outbound) ┐         [admission: L2 nonce+balance]
                                                                          │
                                  ╔════════════════════════════════════════╗
                                  ║  HeldPool — direction-tagged            ║  (one pool; per-direction nonce
                                  ║  contiguity keyed (sender, direction)   ║   index, keyed by proxyEntryHash/tag)
                                  ╚════════════════════════════════════════╝
                                      │ each Sync slot: drain BOTH; compose each by direction (discriminator: proxyEntryHash)
                       ┌──────────────┴───────────────┐
              inbound  ▼                       outbound ▼
   composer INBOUND (entry=L1) ─▶ system-tx L2   composer OUTBOUND (entry=L2) ─▶ l2ToL1Calls entries
                       └──────────────┬───────────────┘
                                      ▼  prepare_post_batch_raw — ONE MIXED postBatch:
                                         [ anchor | outbound immediates (proxyEntryHash=0, +1 created delta each)
                                                  | inbound deferred (proxyEntryHash=H) ]
                                         transientExecutionEntryCount = 1 + N_outbound
                                      ▼
                       bundle [postBatch, user_txs] ─▶ builder ─▶ L1
```

---

## 1. Verdict (verified in-tree) ✅ with 2 CRITICAL caveats

The structural thesis holds: the composer is **direction-parameterized** (`Composer::builder` /
`ComposerBuilder::new`, `composer.rs:517`/`330` — note `:280` is the FIELD, not the ctor). The L2→L1 **protocol/evm**
machinery is **ported and live but UNEXERCISED** (`build_l1_postbatch` `entries/mod.rs:246`, `SettlesOutbound`
`lib.rs:245`, zk-poster finalize branch `composition.rs:604` — which never fires inbound because the finalize loop
skips the entry rollup `composition.rs:573`), and `EEZ._processNCalls` (`EEZ.sol:945`) **already executes L2→L1 on
L1**. **BUT** the eez-composer **DRAIN** (`composer.rs:996-1316`) has **ZERO outbound support** — so the
composer-side work is genuinely **NEW**, not "config+wiring". And two pieces have **no soundness anchor today**:

- 🔴 **CRITICAL-1 — outbound settlement can fail SILENTLY.** Outbound entries emit **empty `stateDeltas`**
  (`entries/mod.rs:293`) and `prepare_post_batch_raw`'s stitch loop only **re-chains existing** deltas
  (`composer.rs:1808-1816`), never **creates** one. A **zero-value** outbound passes `EtherDeltaMismatch`
  (`EEZ.sol:932`) and runs the call, but `_applyStateDeltas` no-ops on empty deltas (`EEZ.sol:1002-1019`) → **L2
  root silently unsettled, no revert**. A **value-carrying** outbound reverts `EtherDeltaMismatch` → swallowed by
  `attemptApplyImmediate`'s try/catch → `ImmediateEntrySkipped` (`EEZ.sol:387-397`) → **batch still confirms**. So
  A2's exit criterion can silently fail. HARDER than "silent": the prover `verify_settlement_chain` **HARD-BAILS on
  != 1 StateDelta** (`proverd main.rs:507-514`) — an empty-delta outbound entry can **never be proven**. **Fix:
  CREATE one chained StateDelta per outbound entry FROM SCRATCH** (NO `chain_settlement_deltas` exists to port in
  either tree), `newState` = the user-tx-inclusive sync-block root, **+ a fail-closed gate** (never rely on a postBatch revert).
- 🔴 **CRITICAL-2 — the prover outbound gate has no anchor.** `proverd` cannot read L2 logs/receipts
  (`ExecutionWitness` = state/codes/keys/headers, `control.proto:59-64`; block = consensus RLP, `:52`;
  `extract_inbounds` reads `tx.input()` only, `main.rs:397`; validator JSON = roots/statuses, `main.rs:236-242`).
  `EEZ._processNCalls` computes **no on-chain `crossChainCallHash`** — so `l2ToL1Calls[]` has no on-chain hash
  binding; the **`CrossChainCallExecuted` event** (`EEZBase.sol:97`, emitted in `EEZL2.executeCrossChainCall`
  `:200`) is the **sole independent binding and the prover cannot read it.** A gate built from `l2ToL1Calls[]`
  alone re-hashes the composer's own claim → **vacuous**. **Fix: A3 must FIRST add log plumbing** (expose per-block
  `CrossChainCallExecuted` hash/topics to proverd). Until then outbound is **UNGATED** at the prover — do not claim
  soundness parity with inbound.

**Corrected non-symmetries (verified):**
- ⚠️ **R1 — no L2-side delivery system tx; and SKIP is load-bearing AGAINST silent mis-lowering.** The L2→L1 call
  runs INSIDE the L2 user tx (`EEZL2.executeCrossChainCall` `:180`, `external payable`, NOT `onlySystemAddress`).
  Outbound entries carry `destinationRollupId = entry_rollup_id = L2 id` (`composition.rs:605`→`entries/mod.rs:295`)
  **+ non-empty `l2ToL1Calls`** → they **PASS** `build_inbound_system_txs`'s filters (`system_tx.rs:88,91`) and would
  be **silently lowered into a wrong `executeIncomingCrossChainCall` (onlySystemAddress) system tx.** The guard MUST
  be **`proxyEntryHash`/direction**, NOT entry-shape.
- ⚠️ **R2 — prover recovery needs NEW plumbing (see CRITICAL-2).**
- ⚠️ **R6 — deriver L2→L1 validation is NEW** (replay reuses; logs are produced in re-exec but the receipts in
  `BlockBuilderOutcome` are **dropped** `deriver.rs:429-433` — A4 must capture them, not "query receipts").

## 2. Symmetry table (corrected REUSE/NEW)

| Piece | L1→L2 (exists) | L2→L1 mirror | Class | Effort |
|---|---|---|---|---|
| Entry endpoint | `:18688` ingress (dev hack) / **should be** the interceptor | `:18688` ingress (correct: L2 net) | EXTEND | — |
| Composer instance | `builder(_, l1_rollup_id)` entry=L1 (`main.rs:573`) | ~~2nd composer~~ **STRUCK (Q2/N1): ONE Composer, per-composition entry select** (entry=L1 inbound, entry=L2 outbound) | NEW | M |
| `root_reader` | clones the L1 entry client (`main.rs:472-491`) | **must be a separate EvmL1Style L1 follower** (`stored_target_state_root` Unavailable for non-L1, `client.rs:571`) | **NEW** | S |
| Ingress classify | proxy/chain-id → CrossChain | L2-proxy set | REUSE | 0 |
| HeldPool admission | L1-context nonce+balance (`ingress.rs:162-201`) | **L2-context nonce+balance gate** (new); per-(sender,direction) contiguity | **NEW** | S |
| Drain → lowering | `build_inbound_system_txs` (unconditional, `composer.rs:1094`) | **direction branch**: outbound → settlement splice, NOT system-tx | **NEW** | M |
| Protocol settlement builder | n/a | `build_l1_postbatch`/`SettlesOutbound` (ported) | REUSE | 0 |
| finalize routing | L2 target loop | zk-poster L1 branch (`composition.rs:604`) | REUSE | 0 |
| postBatch assembly | source-only merge, `transientExecutionEntryCount=1` (`composer.rs:1837`) | **splice outbound entries + CREATE 1 delta each + `transientExecutionEntryCount=1+N`** | **NEW** | M |
| On-chain exec | EEZ inbound queue / EEZL2 | `EEZ._processNCalls` (note: reverts SWALLOWED `:387-397`) | REUSE | 0 |
| Source proxies | L1 proxies (`devnet-test.sh:110`) | L2 proxies via `EEZL2.createCrossChainProxy(l1_addr,0)` | EXTEND (script) | S |
| Deriver replay | `execute_block` | same | REUSE | 0 |
| Deriver validation | inbound system-tx reconstruction | **capture L2 logs + validate `l2ToL1Calls` vs exec** | **NEW** | M |
| Prover outcome gate | `inbound_outcome_gate` (`main.rs:416`) | **NEW gate + NEW log plumbing** (not a cheap mirror — CRITICAL-2) | **NEW** | M |
| Prover settle-chain / publicInputsHash / #10 | — | direction-agnostic (`main.rs:279`,`516`,`363`) **but** chain bails on zero-delta entries (`:507`) so CRITICAL-1's delta-create is a prerequisite | REUSE* | 0 |

## 3. Non-breaking design (corrected — keys are NOT shape-disjoint)

L1→L2 stays intact via **ONE Composer with per-composition entry selection** (Q2/N1 — NOT a second instance, NOT
single-direction-per-slot; superseded). **Correction: the detection keys are NOT disjoint by entry shape** —
inbound-deferred and outbound-immediate entries both have
`destinationRollupId = L2` and non-empty `l2ToL1Calls`; the **only** discriminator is `proxyEntryHash`
(inbound H≠0; outbound ZERO, `entries/mod.rs:294`). Non-breaking holds **only if**:
- (a) `prepare_post_batch_raw` **branches** so the inbound path keeps source-only merge + `transientExecutionEntryCount=1` (byte-identical to today);
- (b) the outbound splice/lowering is **strictly conditioned on `proxyEntryHash==ZERO` outbound entries** (the direction branch in the drain, R1);
- (c) the **deriver** subtracts `1 + N_outbound` (not 1) before truncate (`deriver.rs:951`) and excludes outbound immediates (`proxyEntryHash==ZERO`) from `build_inbound_system_txs` — **REQUIRED** (the mixed batch produces both kinds, so this is the crux, not an edge case);
- (d) a **byte-identical inbound regression test** gates "non-breaking" (an inbound-only batch must serialize exactly as today).

The HeldPool's per-tx poison-isolation protects inbound from outbound **only after** the outbound branch participates in the same per-tx eviction (today an outbound tx is a silent zero-entry survivor — `system_tx.rs:88`).

## 4. Phased plan ([CC] = consensus-critical) — extend-eez0, ONE composer, both directions together

Model (decided): **extend eez0's DA/replay** (NOT based sealed-block-replay) — transport the outbound user tx in the
sync-block DA slot, execute it in `build_sync_block`, reconstruct+replay it in the deriver, ONE canonical tx order
across composer + deriver + native-validate; **local** L2-follower source-sim; **one** `Composer` with per-composition
entry selection. First cut **N=1** (single L2→L1 call per entry → chain reduces to `{r0→rn}`, no interior roots).

- **P-1 [REFRAMED by `wf_a4aa0f39` — it is A2's EXIT ASSERTION, not a pre-A2 gate].** Verified in-tree:
  `build_sync_block` **== the deriver BY CONSTRUCTION** (same reth `BlockBuilder`, same `EthEvmConfig`, same
  attributes, same `suggested_fee_recipient = Address::ZERO`, `composer.rs:598` == `deriver.rs:383`) → no
  executor/config divergence is possible. And the **source-sim produces NO root** (`simulate_source_tx` captures
  `(gas, success, proxy_lookups, error)` and `drop(evm)`, never `compute_state_root`, `client.rs:487-497`) — its job
  is **detection + admission**, not root production. So P-1 is NOT "source-sim root == build root" (category error),
  and the **"fall back to gRPC" fear is dissolved** — there is no root-divergence failure mode for the local follower.
  P-1's real form: **after A2 wires the user tx into `build_sync_block` + settles the user-tx-inclusive root**, a fresh
  FOLLOWER re-derives from L1 and its `to_block` root must equal the on-chain attested `newState` (the existing
  harness `wait_for_node_caught_up`, `common/mod.rs:1006-1023`). Level: **anvil-integration**. **The REAL risk is
  tx-list/order consistency** — the deriver must reconstruct the `loadExecutionTable` **byte-identical** to the
  composer's, with the user tx in canonical order `[loadExecutionTable | user_tx]`; P-1's localizing sub-assertion is
  that byte-equality (the only way the otherwise-identical executors can diverge). A **negative control** (a wrong-
  nonce/order outbound the deriver rejects) guards against a vacuous pass.
- **A0 ✅ DONE** (`979d19b`, `443875e`, `de359bf`) — direction-aware pool: classifier reports `Direction`
  (proxy→Outbound, chain-id→Inbound); `HeldTx.direction`; contiguity keyed `(sender, direction)` at all 5 call sites
  + the in-mem `keep.retain` cascade; R8 isolation test; lib tests 30/30, behavior-preserving (all Inbound today).
  The L2-proxy set is already env-driven (`EEZ_CROSS_CHAIN_PROXY_ADDRESSES`, parsed `main.rs:976`, empty today).
  `MAINNET_ROLLUP_ID` folds into A1 where it's used. **L2-context admission gate DROPPED (Edu) — unnecessary:** the
  inbound gate protects the all-or-nothing L1 bundle (`composer.rs:1306` `bundle.extend(survivors.raw_tx)`; the
  inbound user tx is L1-signed + executes on L1). The OUTBOUND user tx is L2-signed → **never rides the L1 bundle**
  (it executes in the L2 sync block, extend-eez0); a bad outbound is handled by the existing compose-time poison-evict
  (now direction-keyed). A gate would be at most a UX nicety, not worth the self-referential `EEZ_L2_RPC_URL` provider
  + startup-ordering risk.
- **A1** — **one composer, per-composition entry selection**: choose `(entry=L1, L1 client)` for inbound and
  `(entry=L2, L2-follower)` for outbound per drained tx (feasible — `entry_rollup_id` is per-composition at
  `CompositionBuilder`, `composition.rs:299`). §2 "second composer" row is **struck**. Exit: node boots; inbound
  byte-identical; an outbound tx composes an L1 settlement batch (not yet assembled).
- **A2** — outbound execution + transport + single mixed assembly:
  - **(a0) [SPEC — was MISSED, CRITICAL] L2-side consumption via `loadExecutionTable`.** The outbound user tx's
    `executeCrossChainCall` CONSUMES an **L2 DEFERRED entry** that MUST be loaded by a SYSTEM_ADDRESS
    `loadExecutionTable` tx **in the same block, BEFORE** the user tx — else it reverts `ExecutionNotFound`
    (`EEZL2.sol src/L2/:403-409`; same-block gate `:189`), so A2's exit and the whole P-1 root bet are unreachable.
    Build the outbound L2 deferred entry (`proxyEntryHash = computeCrossChainCallHash(MAINNET(0), target, value,
    data, source, L2)`; `rollingHash` per `CALL_BEGIN(1)→CALL_END(1,true,ret)`; `callCount=1`) via the SHARED
    constructor **A2.1 ✅ BUILT** (`build_l2_outbound_entry` + `build_outbound_load_table_txs`, `06a067b`/`038698c`).
    **Correction (was wrong):** there is NO standalone `loadExecutionTable` system tx today — `addresses.rs:14` is a
    doc comment, and inbound loads the table INTERNALLY inside `executeIncomingCrossChainCall`. The outbound loader is
    genuinely new (per-entry, SyncPair-interleaved); the deriver reconstructs via the SAME shared fn (a NEW path,
    distinct from `executeIncomingCrossChainCall`). *(Spec: `EXECUTION_ENTRY_SPEC.md:425-438`.)*
  - **(a)** `build_sync_block` orders `[loadExecutionTable | outbound user tx]` and **executes the user tx** (beyond system-txs-only).
  - **(b)** transport the outbound user-tx bytes in the sync-block DA slot (today empty `composer.rs:1969`).
  - **(b1) [verified] the L1 bundle is inbound-only.** `composer.rs:1306` `bundle.extend(survivors.raw_tx)` puts ALL
    survivor user txs into the L1 bundle — correct for inbound (L1-signed, executes on L1), WRONG for outbound
    (L2-signed, would be invalid on L1). Branch: outbound survivors go into the **L2 sync block**, NOT the L1 bundle.
    An outbound-only slot's bundle = `[postBatch]`; a mixed slot = `[postBatch | inbound survivors]`.
  - **(c)** `prepare_post_batch_raw` **splices** outbound entries into `batch.inner.entries` → `[anchor | outbound(0)
    | inbound(H)]`, and **CREATES one chained StateDelta per outbound entry FROM SCRATCH** (there is **NO**
    `chain_settlement_deltas` to port — it exists in neither tree): `{rollupId=L2, currentState=prior chained newState
    (r0=L1-committed), newState = the USER-TX-INCLUSIVE sync-block root (P-1's deriver `to_block` root — NOT the
    system-tx-only root that `composer.rs:1824-1831` overwrites to), etherDelta=0}`; `transientExecutionEntryCount=1+N`.
  - **(d)** deriver reconstructs+replays the outbound user tx; gates `retain()` AND `build_inbound_system_txs` on
    **`proxyEntryHash != 0`** (NOT `l2ToL1Calls` emptiness, `deriver.rs:950`), and counts the actual leading
    `proxyEntryHash==0` run for `consumed_deferred` (NOT `-1`, `deriver.rs:951`).
  - Fail-closed: assert ZERO `ImmediateEntrySkipped` **AND** proverd `verify_settlement_chain` success (it HARD-BAILS
    on != 1 StateDelta). Exit: one **state-mutating** outbound `setValue` settles on L1; a fresh follower re-derives the same L2 root.
- **A2b [CC] — real inbound coexistence (N4) — ✅ DONE + VALIDATED (2026-06-22).** An E2E with a REAL inbound L1→L2
  delivery AND an outbound L2→L1 call in the SAME slot, both re-derived independently by a fresh follower from L1.
  `e2e_mixed.rs` GREEN (84s): both legs settle (L1 Value==42 / L2 Value==43, distinct ⇒ no cross-wiring), L1
  `rollups[1].stateRoot`==L2 safe root at a mixed-settlement-inclusive height, the follower height-pins the exact mixed
  root AND its deriver log shows `outbound>0 && inbound>0` on ONE reconcile line. Single-direction stays byte-identical
  (e2e_outbound 2/2 + e2e_inbound 1/1). **Fixed BY CONSTRUCTION, not by patching the 4 forks (B1 sidecar-XOR / B2
  tx-order / B3 two-phase nonce / B4 loadTable self-clean):** both composer & deriver build the Sync block through the
  SINGLE shared `eez_evm::system_tx::build_cross_chain_sync_pairs` → `interleave_sync_block_txs`; the DA sidecar carries
  BOTH directions always (no XOR); the deriver pairs outbound entries positionally with the Sync block's user txs.
  Commits `379cf44` (builder) → `aee276d` (composer+deriver migration) → `efa7795` (test). **Change-4 (c) DONE
  (`92fa6b2`):** the deriver no longer truncates the inbound FIFO by `settled_count - (1 + N_outbound)` (which deflates
  by one per skipped outbound immediate, EEZ.sol:392, wrongly dropping an inbound delivery). It now counts the
  AUTHORITATIVE `ExecutionConsumed` event (EEZ.sol:903 — fires once per consumed inbound deferred, never for an anchor
  or outbound immediate): submitter scans it (rollupId=topic2) → `consumed_count` threaded via L1Event::BatchPosted to
  `consumed_deferred = consumed_count`, independent of the outbound count. DERIVER-ONLY (composer entry-agnostic);
  `settled_count` kept for the finality gate. Mirrors based-rollup's per-entry ExecutionConsumed §4f counting.
  Happy-path byte-identical (consumed_count == settled_count-(1+K) with no skip) — all 3 e2e green, mixed root
  unchanged; new unit tests cover the per-block tally + the skip divergence. NOTE: a skip can't reach this path in a
  passing flow (P-1's fresh-follower re-derivation fails first), so this is defensive hardening that removes the
  static-N coupling, not a bug reachable today.
- **A3 [CC]** — prover outbound gate (**FULL L2-source validation**, per Q6): the proof must **independently
  validate the L2 source authorization (L2-log-vs-exec)** — not merely bind the topic. Expose per-block
  `CrossChainCallExecuted` (`topic0=H`) from the authoritative re-execution AND cross-check it against the replayed
  L2 execution; recompute `H` per `EEZL2.sol:197-199` (`sourceRollupId=L2`); constrain/thread `targetRollupId`
  (`L2ToL1CallSol` omits it). **A3 sub-decision:** which re-execution is authoritative — native-validate (the proof's
  own) vs the deriver; P-1 already requires the two agree on the root. Until A3 lands, outbound UNGATED (dev/testnet
  only; the fail-closed no-`ImmediateEntrySkipped` assert is the interim backstop).
- **A4 [CC]** — deriver outbound validation: **capture L2 logs** (receipts dropped at `deriver.rs:429-433`) +
  validate `l2ToL1Calls` vs replayed exec; reject before commit. (Folded with A2b's deriver work.)
- **A5** — **N≥2 multi-call** (eez0 must add per-prefix interior-root sealing à la based `build.rs:538-695`; today
  `build_sync_block` seals once `local/build.rs:140-146`) + value-carrying (`etherDelta=-outbound_ether_out`) + hardening.
  - **Value-carrying INBOUND (L1->L2) DONE (2026-06-22, commit `441962f`).** An inbound call carries ETH: user
    attaches `msg.value=V` on L1, V is escrowed on L1 (rollup `etherBalance += V`) and delivered to the L2 target.
    The plumbing was ~90% wired (entry builders take `value`, sidecar carries `l2ToL1Calls[0].value`, L2 delivery
    attaches it from the pre-funded SYSTEM_ADDRESS, proxyEntryHash binds it); the one gap was the lean inbound entry's
    settlement `etherDelta` (was `I256::ZERO`). Fix: `prepare_post_batch_raw` builds a `proxyEntryHash -> +value` map
    from the sidecar entries (same hash preimage for L1-originated inbound) and sets `etherDelta` from it — so the
    bundled consume satisfies `totalEtherDelta == _entryEtherIn - etherOut` (`+V == V - 0`). Prover-orthogonal
    (ether-agnostic), DERIVER unchanged (already reconstructs the value delivery from the sidecar). `e2e_value_inbound.rs`
    green (0.5 ETH+7wei deposit lands on L2 `ValuePayable` + follower re-derives); regression byte-identical
    (inbound/mixed/outbound, value=0 -> etherDelta=0).
  - **Value-carrying OUTBOUND (L2->L1) withdrawal DONE (2026-06-22, commit `728ac7a`).** The mirror: an outbound entry
    is an immediate (`_entryEtherIn==0`), so `totalEtherDelta == -etherOut` forces `etherDelta = -M`. The composer's
    outbound splice now books that debit via the existing `outbound_ether_out` helper (recovers M, or 0 for
    value-free/failed, from the rollingHash); multi-call-with-value -> None -> rejected. The L2 burn to SYSTEM_ADDRESS
    was already wired. The rollup's L1 `etherBalance` is funded ONLY by a prior inbound deposit (+V), so a withdrawal
    requires one — `e2e_value_outbound.rs` runs the full canonical cycle: DEPOSIT 1 ETH (reserve += 1 ETH, lands on L2)
    then WITHDRAW 0.4 ETH+9wei (L1 target balance += M, reserve == V-M, follower re-derives both batches). Regression
    byte-identical (e2e_outbound 2/2 + e2e_mixed 1/1; inbound-only paths don't touch the outbound splice).
    **value-bearing cross-chain is now COMPLETE** (deposits + withdrawals with bridge-reserve accounting), prover-
    orthogonal. NEXT: N>=2 multi-call value (per-call success extraction from the folded rollingHash) + multi-L2.

**Track B — proper L1→L2 entry (the interceptor)** — for wallet-driven L1→L2:
- **B0** — port based's `l1_interceptor` as an axum L1-RPC front (`:18646`): forward all `eth_*` to `:18645`, intercept `eth_sendRawTransaction`, detect L1→L2 (simulate vs L1), **detect + PUSH(dir=inbound) — NOT `orchestrate_inbound_l1`**. **Admission MUST replicate eez0's FULL ingress gate** (nonce == L1 on_chain + held, balance) — based's interceptor only checks nonce-too-low (`l1_interceptor.rs:185-210`), strictly weaker; factor `ingress.rs:155-202` into a shared helper. Exit: a real L1 wallet submits L1→L2 through `:18646`; lands in HeldPool; existing inbound path delivers it.
- **B1** — keep/deprecate the `:18688` chain-id L1→L2 hack (dev-only); document the interceptor as the production entry.

Tracks A and B are independent and converge at the HeldPool. A2 is the first observable L2→L1; B0 the first wallet-correct L1→L2.

## 5. Risks

| # | Sev | Risk | Mitigation |
|---|---|---|---|
| R1 | MAJOR | Drain silently mis-lowers outbound into an inbound system tx (entry-shape passes `build_inbound_system_txs`). | Direction branch keyed on `proxyEntryHash`/tag; treat "outbound ok but 0 L1 entries" as poison, not a silent survivor. |
| R3 | MAJOR | Ordering: outbound immediates (`proxyEntryHash==0`) must be the **contiguous leading run** after the anchor, before any deferred; a mis-set `transientExecutionEntryCount` or an outbound landing in the remainder → published to `executionQueue`, never executed. | Lay out `[anchor | outbound_1..N | deferred...]`, `transientExecutionEntryCount=1+N`. |
| R8 | MAJOR | Unified-pool **nonce-context collision**: one per-sender contiguity index mixes L1-context (inbound) and L2-context (outbound) nonces for the same EOA. | Key contiguity on `(sender, direction)` — **✅ DONE (`de359bf`)**. NO L2-admission gate needed: outbound never rides the L1 bundle (`composer.rs:1306`), so a bad outbound is just compose-time poison-evicted (direction-keyed) — no bundle to protect. |
| R9 | **CRITICAL** | **Silent unsettled root / silent skip** (CRITICAL-1). | CREATE a chained settlement delta per outbound entry; fail-closed verify (assert no `ImmediateEntrySkipped`). |
| R10 | **CRITICAL** | **Prover outbound gate has no soundness anchor** (CRITICAL-2) — proverd can't read L2 logs. | A3 adds log plumbing first; outbound UNGATED until then; do not advertise parity. |
| R11 | MINOR | Outbound `destinationRollupId = L2` is **intentional** (passes `_validateStructure` because the L2 id stays in `rollupIdsWithProofSystems`). | Document — do NOT "fix" to the L1 id. |
| R4 | MINOR | "Single L1 client" is imprecise — distinct `LocalChainClient`s share the same read-only reth provider; serialization comes from the one-in-flight gate + sequential slot loop, not a shared object. | One postBatch per rollup per slot (unified pool, single drain), enforced by the existing gate. |

## 6. Decision + open questions (ranked)

**DECIDED (Edu) — direction strategy: BOTH directions in one postBatch (mixed batch).** "based does one direction
per slot" is a reference choice, not a correctness requirement; eez0 settles whatever the L2 block contains. The
contract already tolerates the mixed layout (`EEZ.sol:387-397`/`672-676`). This is NEW R&D (the composer/deriver/
prover work in §4 A2/A2b/A4), fully characterized — not "reuse". The outbound-alone increment (A2) is a de-risking
stepping stone on the way to the mix (A2b), not a different destination.

**Ranked open questions:**
1. **Build order** — A2 (outbound-alone increment) → A2b (mixed), or straight to A2b? Recommendation: the increment, to validate outbound composition (delta creation, `_processNCalls`, the L2-proxy) before adding coexistence. Pure engineering hygiene; the destination is the mix either way.
2. **A2 delta creation** (CRITICAL-1) — `prepare_post_batch_raw` must CREATE one chained settlement delta per outbound entry (the stitch only re-chains). Blocks A2 correctness; design before coding.
3. **A3 prover log access** (CRITICAL-2) — pick the mechanism (extend validator JSON vs capture witness re-exec logs) before scoping A3 as a real gate. Until then outbound is UNGATED at the prover.
4. **A4 deriver log capture** — capture the receipts dropped at `deriver.rs:429-433`.
5. **B0 admission parity + cutover** — full ingress admission (not based's weaker nonce-only gate); keep both L1→L2 entry paths during transition or hard-cut to `:18646`. Independent of Track A.
6. **A5 value/multi-call** — `outbound_ether_out` asserts `callCount==1`; wire `etherDelta=-etherOut` into the created delta. Deferred, but the empty-stateDeltas→EtherDeltaMismatch gap is flagged now (R9).

---

## 7. SECOND REVIEW (`wf_d98374c8`, 13-agent, verify-or-ask) — BLOCKED-ON-QUESTIONS

The mixed-batch claim is **half-true and correctly stated**: EEZ.sol does **tolerate** `[anchor | N outbound | M
inbound]` (drain halts at first `proxyEntryHash!=0` `EEZ.sol:387-397`; remainder queued by dest `:672-676`;
`_validateStructure :449-523` enforces NO ordering/direction shape). **But "tolerates" = "does not revert", NOT
"validates"** — `transientExecutionEntryCount` is **not proof-bound** (`:600-608`), so a wrong layout/count loses
entries **SILENTLY** (`BatchPosted` still fires `:420`). Two **architectural gaps not in rev-4**, both CONFIRMED:

- 🔴 **KEYSTONE — the outbound L2 user tx has nowhere to execute.** In-tree there is **NO path** that puts an
  outbound user L2 tx into an L2 block: `build_sync_block` consumes only system txs; the zk-poster composition uses
  L1's **current root as a PLACEHOLDER** `post_state_root` (`composition.rs:603-619`). So the CRITICAL-1 "create a
  delta per outbound entry" fix has **no real, re-executable L2 `newState` to carry** — which breaks the prover
  endpoint gate (`main.rs:521`), the prover interior gate (`:564-575`), AND the **deriver** `check_claimed_state`
  (`deriver.rs:1162`, `claimed_new_state == local to_block root`; the composer overwrites the last newState to the
  system-tx-only `sync_block_state_root` `composer.rs:1824-1831`, which omits the outbound mutation → `local_diverged`).
- 🔴 **No single batch-assembly path for the mix.** Outbound (zk-poster) entries land in `composition.targets[].batch`;
  `prepare_post_batch_raw` merges **only** `compositions[].source.batch` (`composer.rs:1739-1747`). Target entries
  flow only to `build_inbound_system_txs` + the codec DA channel — **never to the postBatch `entries[]`.** The
  splice/delta-create/stitch/overwrite order is **undesigned NEW machinery**, not "wiring".

**All CRITICAL-1/CRITICAL-2/R1/R8 from rev-4 re-confirmed in-tree**, plus: `_applyStateDeltas` `StateRootMismatch`
on a wrong `currentState` is **also swallowed** into `ImmediateEntrySkipped` (`EEZ.sol:1006`/`392-394`) → no
postBatch backstop; `proxyEntryHash==ZERO` is **shared** by outbound-immediate AND inbound settlement-only entries
(`entries/mod.rs:544`/`614`) + the anchor — so "ZERO" means "drain inline", NOT "outbound" (add non-empty
`l2ToL1Calls` to mean outbound); `L2ToL1CallSol` **omits `targetRollupId`** so the prover must assume `=0` (only
valid if every L2→L1 proxy was created with `originalRollupId=0`, which **nothing enforces** today). R8 explicit
sites: `held_pool.rs:124-131`,`:139-157`; `ingress.rs:172-173`; `composer.rs:912`,**`:938` (in-mem retain that
BYPASSES the pool method — easiest to miss)**,`:954`. No L2 provider exists at the ingress layer (`main.rs:259-269`,
constructed before launch); the gate is active against L1.

**Nothing above is a present bug** — only ONE composer is wired (entry=L1, `main.rs:573-579`); the inbound-only path
is intact. These are **hard prerequisites before the outbound composer is wired.**

### Open questions for Edu (NOT to be guessed — answers drive A0–A5)
1. **Where does the outbound L2 user tx execute** so the created delta's `newState` = the deriver's local `to_block`
   root = native-validate's `final_state_root`? Options: (a) **port based's SyncPair** (sync block executes survivor
   user txs as pairs with their system txs; deriver reconstructs for replay) — the only option that closes prover +
   deriver + contract together; (b) **first cut = no L2-state-mutating outbound** (call only burns `msg.value` to
   SYSTEM_ADDRESS), so only the system-tx sync-block root matters; (c) other.
2. **How is the single mixed batch assembled?** (a) **ONE composer (rollup_id=L2)** draining both directions in one
   `prepare_post_batch_raw` (the §2 "second composer" row then stale); (b) **TWO composers + a merge step** splicing
   target+source before one assembly; (c) **SEPARATE postBatches** (revert mixed → one direction per slot; both
   directions still exist, just not in the same block).
3. **Interior boundaries / mixed ±V ether chain** (bites at N≥2 and +V inbound coexisting with −V outbound): real
   re-executed pair-end roots vs `interim_interior_root` placeholders; the per-entry `etherDelta` layout. based's
   `chain_settlement_deltas` has **no** construction for the mix.
4. **Is the inbound runtime-delivery reconstruction path exercised at all today?** Every production L1 inbound
   builder emits **empty `l2ToL1Calls`** (`entries/mod.rs:499`) → skipped/dropped; the code comment
   (`system_tx.rs:98-101`) says "outbound-first does not exercise this delivery path at runtime." If unexercised,
   A2b builds the inbound-coexistence half **from scratch**, not "extends" it. *(Partly verifiable in-tree.)*
5. **HeldPool shape + L2 provider source:** one pool keyed `(sender, direction)` vs two physical pools; and where
   the ingress gets an L2 provider for outbound nonce/balance (new `EEZ_L2_RPC_URL` over `:18688` with a
   startup-ordering story, or post-launch injection of `handle.node.provider`).
6. **Outbound threat model → A3 scope:** is L1 execution of the L2→L1 call **itself** the settlement (only forgery
   risk = a fabricated L2 source ⇒ A3 only binds the `CrossChainCallExecuted` topic), or must L2-source
   authorization be **independently proven** before L1 settles (⇒ A3 needs full L2-log-vs-exec in the proof)? And
   which re-execution is authoritative — native-validate vs the deriver?

*(Citation fixes pending: inspector is `crosschain-composer/src/inspector.rs`; `EEZL2.sol`/`EEZBase.sol` under
`sync-rollups-protocol/src/`; `composition.rs`/`build_settlement_batch` in `eez-protocol`; strike the stale
`chain_settlement_deltas` ref at `entries/mod.rs:610` — it is a based-only function.)*

### DECISIONS (Edu, 2026-06-20) — resolves Q1 + Q2

- **Q1 → port based's SyncPair model.** The composer's sync block must **execute the survivor user txs** (the
  outbound L2 user txs) interleaved with their system txs as **pairs**, and the **deriver must reconstruct the user
  tx for replay** — so the created outbound delta's `newState`, the deriver's local `to_block` root, and
  native-validate's `final_state_root` are the **same** root. This replaces eez0's system-txs-only `build_sync_block`
  and the deriver's system-tx-only reconstruction. *(This is a partial return to based's general model that eez0
  deliberately deferred — see the Phase-B inbound.rs deviation in STATE-vs-PLAN.md; porting it for outbound likely
  also lands the deferred inbound completeness.)*
- **Q2 → ONE composer (rollup_id=L2)** drains **both directions** from the unified pool and emits the mixed batch
  via a **single** `prepare_post_batch_raw` laying out `[anchor | outbound(0) | inbound(H)]`,
  `transientExecutionEntryCount=1+N`. **The §2 "second composer" row is STRUCK.** *(Open sub-detail — verify, do not
  guess: eez0's `Composer` fixes `entry_rollup_id` in the builder, but inbound needs entry=L1 and outbound entry=L2;
  the one-composer model requires `entry_rollup_id` to be per-COMPOSITION, not per-instance. Confirm how based's
  `service.rs` does this before coding A1.)*

**Still open (next batch):** Q3 (interior/±V chain, N≥2), Q4 (is inbound delivery reconstruction exercised today —
being verified in-tree), Q5 (HeldPool shape + L2-provider source), Q6 (outbound threat model → A3 scope). These are
resolved after the SyncPair shape is verified.

### VERIFICATION (`wf_5588c419`, 7-agent verify-or-ask) — the SyncPair port is NOT a clean lift

**Verified facts (file:line):**
- **SyncPair shape (based).** `SyncPair { system_tx_calldata, user_tx, value }` (`composer.proto:178`). The composer
  builds ONE pair per held tx (`service.rs:551-694`); the reth-node seals them **interleaved system-then-user**
  `[s1,u1,s2,u2,…]` in one growing prefix (`build.rs:447-588`); per-pair **interior roots** `[R1..Rn]` come from
  per-prefix re-seals on a JoinSet (`build.rs:538-695`) exposed via `eez_candidateRoots` RPC; `chain_settlement_deltas`
  attaches one chained delta per entry with `P0=r0` (L1-committed) … `Pn=rn` (sealed L2) (`post_batch_submitter.rs:278-334`).
- **based has NO L1-deriver.** Validation/proving **replays the full sealed block** (system+user txs in the witness
  block RLP, `witness_rpc.rs:84-101`). The user tx for replay is already present — never reconstructed from L1.
- **based does NOT run "one composer" over both directions.** It uses an entry=L1 `Composer` for inbound +
  a **per-tx** entry=L2 `CompositionBuilder` for outbound (`service.rs:589`), and outbound source-sim runs on the
  **real L2 node via a gRPC stream** (`BeginSimulate(SourceTx)`, `service.rs:600-630`), NOT a local
  `simulate_source_tx`. `entry_rollup_id` is per-composition at the `CompositionBuilder` level (per-instance only at
  the `Composer` wrapper).
- **Q4 ANSWER = PARTIAL / effectively greenfield.** The deriver reconstructs inbound from `decoded.l2_entries`
  (the DA callData L2-shape entries), NOT the on-chain `entries[]` (codec-v1 fallback). The reconstruction CODE runs
  in composer mode **only as a self-consistency byte-check** against a self-built block; **no test/E2E/deployment
  ever independently re-derives a REAL inbound** (the live Chiado slice was outbound/empty — `STATE-vs-PLAN.md:30`).
  ⇒ **even the INBOUND path is not deriver-validated** end-to-end; the A2b inbound-coexistence half is greenfield-validated.

**THE divergence (why it's not a lift):** based = **sealed-block-replay** (full block in DA, interleaved order,
gRPC source-sim, two composer objects). eez0 = **entry→system-tx reconstruction** (empty sync-block DA slot
`composer.rs:1969`, system-first concat `deriver.rs:1006-1018`, local source-sim, one pinned composer entry=L1).
For N≥2 or any cross-tx dependency these produce **different roots**. "Porting SyncPair" forces choosing a model.

### NEW questions for Edu (emerged from verification — not derivable from code)
- **N1 — "one composer" (clarify Q2):** one ASSEMBLY path (single `prepare_post_batch_raw`) allowing based-style
  TWO composition objects [based-proven], or strictly ONE `Composer` object with per-composition entry selection [novel]?
- **N2 — outbound source-sim:** local L2-follower `simulate_source_tx` [smaller diff; must confirm it yields a
  re-executable mutation matching the deriver root], or based's gRPC stream where the **real L2 node** runs it [based-proven]?
- **N3 — sync-block DA/replay model (the keystone):** adopt based's **sealed-block-replay** (full sync block —
  system+user, interleaved — in DA; deriver replays it; drop entry→system-tx reconstruction for sync blocks), or
  **extend eez0's model** (keep reconstruction, ADD outbound user-tx transport+execute+replay with ONE canonical order)?
- **N4 — sequencing:** the deriver's independent inbound re-derivation is UNEXERCISED — validate a REAL inbound E2E
  through the deriver (the foundation) BEFORE building the mixed batch atop it, or build both together?

### DECISIONS round 2 (Edu, 2026-06-20) — resolves N3 + N4 (+ N1/N2 by implication)
- **N3 → EXTEND eez0's model** (NOT based sealed-block-replay). Keep entry→system-tx reconstruction; ADD the outbound
  user tx to the sync-block DA slot (today empty `composer.rs:1969`), EXECUTE it in `build_sync_block`,
  RECONSTRUCT+replay it in the deriver, under ONE canonical tx order identical across composer + deriver +
  native-validate.
- **N4 → BUILD both directions together**; validate a REAL inbound (independently re-derived by a fresh follower) as
  part of A2b — the foundation is currently unexercised (Q4 = PARTIAL).
- **N1 (by implication of Q2 + N3) → ONE `Composer` with per-COMPOSITION entry selection** (`entry=L1` inbound,
  `entry=L2` outbound) — feasible: `entry_rollup_id` is per-composition at `CompositionBuilder` (`composition.rs:299`).
- **N2 (by implication of N3) → LOCAL source-sim** (the L2-follower client runs the outbound source-sim).
- 🔴 **KEY RISK (P-1, prove FIRST):** the L2-follower local source-sim MUST produce the SAME re-executable root the
  deriver computes at `to_block` and native-validate as `final_state_root`. based avoided this via gRPC on the real
  L2 node; eez0's local bet is unproven. **A root-equality test gates the whole effort; if it fails → N2=gRPC.**
- **SCOPE (resolves Q3 first cut):** N=1 (single L2→L1 call per entry) → settlement chain reduces to `{r0→rn}`, NO
  interior roots (eez0's single-shot `build_sync_block` produces none). N≥2 deferred to A5.

### DECISIONS round 3 (Edu, 2026-06-20) — resolves Q5 + Q6 (last forks)
- **Q5 → ONE pool keyed `(sender, direction)`** (not two physical pools); a `direction` tag on `HeldTx`. **✅ DONE
  (A0, `de359bf`).** The L2-provider-for-admission part is **MOOT** — the L2-context admission gate was dropped (see
  A0): outbound never rides the L1 bundle, so there is no all-or-nothing bundle to protect at the door; a bad outbound
  is compose-time poison-evicted. No `EEZ_L2_RPC_URL` self-reference needed.
- **Q6 → PROVE L2-source authorization before L1 settles.** A3 = **full L2-log-vs-exec validation inside the proof**
  (binding the `CrossChainCallExecuted` topic alone is insufficient). Stronger soundness, more A3 work. Open A3
  sub-decision: which re-execution is authoritative (native-validate vs deriver) — P-1 requires the two agree on the root.

**All forks resolved.** The plan (§4 phases, gated by **P-1**) is executable. Remaining = the A3 authoritative-re-exec
sub-decision (an A3-phase detail) + the citation fixes above.

---

## 8. THIRD REVIEW (`wf_880a2453`, 11-agent, plan vs PROTOCOL SPEC) — GAPS-FOUND

Reviewed rev-9 against `sync-rollups-protocol/docs/{EXECUTION_ENTRY,CORE_PROTOCOL,LOOKUP,MULTI_PROVER,CAVEATS}` +
`DERIVATION.md`. **Verdict: GAPS-FOUND.** The L1-settlement side is spec-legal, but the plan **missed the L2-side
consumption mechanism** the protocol mandates. Two real VIOLATES-SPEC (both now folded into §4):

- 🔴 **V1 [now A2(a0)] — the L2 `loadExecutionTable` / deferred-entry was MISSING.** An L2→L1 call's L2 leg is a
  DEFERRED entry that MUST be loaded by a `loadExecutionTable` system tx **before** the user tx's
  `executeCrossChainCall`, or it reverts `ExecutionNotFound` (`EXECUTION_ENTRY_SPEC.md:425-438`; `EEZL2.sol:403-409`).
  The plan never mentioned it → A2 was literally unrunnable. **eez0 already opens every Sync block with a per-slot
  `loadExecutionTable` tx (`addresses.rs:14-18`)** — wire the outbound entry in + deriver reconstructs it.
- 🔴 **V2 [now an A0 admission gate] — reverting outbound silently dropped.** `build_l1_postbatch` SKIPS reverted
  top-level L2→L1 calls (`entries/mod.rs:264-271`) with no poison gate. The spec requires reverting outbound be
  expressible (`CALL_END(false,…)`/`revertSpan`, `EXECUTION_ENTRY_SPEC.md:39,511-535`).

**MUST-HANDLE-NOW (N=1) — folded into §4 A2/A4:** (1) the `loadExecutionTable` wiring [A2(a0)]; (2) create one
StateDelta per outbound entry, `newState` = user-tx-inclusive root, **prover HARD-BAILS on != 1 delta** [A2(c)];
(3) the single batch-assembly splice (undesigned) [A2(c)]; (4) deriver `retain()`+`build_inbound_system_txs`+count
gated on `proxyEntryHash != 0`, bound to `deriver.rs:950-951` [A2(d)/A4].

**DEFERRED-RISKY → require an A0 ADMISSION HARD-REJECT (poison, not silent drop) until A5:** any outbound whose
source-sim shows **(a) a top-level revert**, **(b) a reentrant cross-chain call** (`L2→L1→L2` needs
`expectedL1ToL2Calls` + nested rolling hash, `EXECUTION_ENTRY_SPEC.md:477-509` — unexpressible in N=1), or
**(c) `msg.value > 0`** (etherDelta accounting: value is burned to SYSTEM_ADDRESS on L2 `EEZL2.sol:191-195`; the
created delta would need `etherDelta=-etherOut`, `EXECUTION_ENTRY_SPEC.md:286-294`). Each is a **real gate + test**, not a comment.

**A3 scope (sharpened, per Q6=full validation):** the gate must re-derive the L2-source authorization from a full L2
re-execution and compare the **FULL entry** (`rollingHash` incl. `returnData`+success, the `L2ToL1Call` fields, the
created StateDelta) — not just topic0/root. Must **VERIFY** the source proxy's `originalRollupId == MAINNET(0)`
(`createCrossChainProxy` accepts any; nothing enforces 0; `L2ToL1CallSol` omits `targetRollupId`). native-validate &
the deriver must share **identical STF inputs** so they can't disagree on the outbound root.

**DEFERRED-SAFE (A5):** N≥2 multi-call + `expectedLookups` disambiguation + failed/static lookups + per-prefix
interior roots + value-carrying. `transientExecutionEntryCount` not proof-bound — layout discipline + the
no-`ImmediateEntrySkipped` assert are the only mitigation (plan owns it).

**Internal fixes:** A2 `loadExecutionTable` + the `chain_settlement_deltas` recipe + the §2 struck row + the §3 lead
are corrected above. **Still TODO (cosmetic):** inspector path → `crates/eez-evm-inspector/src/inspector.rs`;
`EEZL2.sol`→`src/L2/EEZL2.sol` in the body. **P-1 per-phase gating:** A0 (env/pool) + A1 (entry-selection wiring) are
P-1-INDEPENDENT (may start now); **A2+ are P-1-gated** (a P-1 failure → N2=gRPC re-plan).

### DECISION (Q7, Edu) — RESOLVED
**N=1 first-cut outbound = STATE-MUTATING VALUE-FREE** (e.g. `setValue` writing L2 storage, `etherDelta=0`) so P-1
proves something non-vacuously. **value>0 / naturally-reverting / reentrant L2→L1→L2 outbound are HARD-REJECTED
(poison) at admission** — a real gate + test, not a comment — until A5. **All questions resolved; the plan is
spec-reviewed and executable.**

---

## 9. P-1 DESIGN + DECISIONS round 4 (Edu, `wf_a4aa0f39`)

**Verified:** `build_sync_block` == the deriver BY CONSTRUCTION (same executor/config/fee-recipient); the source-sim
produces NO root (detection+admission only). So the gRPC-fallback fear is dissolved; the only divergence mode is
tx-list/order. Decisions:
- **Q1 → P-1 = A2's EXIT ASSERTION** (not a pre-A2 gate). A2 is built first; P-1 is its acceptance test: a `setValue`
  outbound → a fresh follower re-derives from L1 → its `to_block` root == the on-chain attested `newState` (extends
  `wait_for_node_caught_up`, anvil-integration) + a `loadExecutionTable` byte-equality sub-assertion + a negative control.
- **Q4 → ONE SHARED `loadExecutionTable` constructor** for the outbound L2 deferred entry, called by BOTH the composer
  and the deriver → byte-identical by construction (mirrors how inbound shares `build_inbound_system_txs`). P-1 asserts
  the byte equality.
- **Minor (confirmed):** the outbound user tx is signed by a **distinct L2 EOA** (own nonce sequence → the source-sim's
  `disable_nonce_check` skew is irrelevant); P-1's **native-validate leg** is anvil-sufficient for the local-follower
  bet (the proverd gate enforces `settled==final_state_root` at runtime; proverd-in-loop is a later confirmation); the
  reth `finish()` `header.state_root==trie-root` invariant is **trusted** (proverd exercises it transitively).

### A2 decomposition (the main consensus-critical work; build then P-1)
- **A2.1 ✅ DONE** (`06a067b`, `038698c`) — the SHARED outbound `loadExecutionTable` constructor:
  `build_l2_outbound_entry` (the lean L2 deferred entry, `proxyEntryHash = cross_chain_call_hash(MAINNET(0), target,
  value, data, source, L2)`, `rollingHash`, `callCount=1`) + `build_outbound_load_table_txs` (the per-entry
  `loadExecutionTable` system-tx builder, shared composer↔deriver). Byte-exact unit tests (direction, rolling fold,
  emit==deriver-rebuild). No wiring yet (A2.3/A2.4 consume it). Decisions: #1 hardcode MAINNET, #2 returnData param
  (non-void tested), #3 per-entry (SyncPair-interleaved).
- **A2.2 ✅ DONE** (`1184767`) — `SyncPair` + `interleave_sync_block_txs` (the canonical `[s,u?,…]` order, shared).
- **A2.3 ✅ DONE** (`979d19b`…`bb46742`) — composer wiring (design `wf_f1166251`; decisions below). **CRITICAL-1 CONFIRMED:** must CREATE one
  StateDelta per outbound entry (`build_sync_block` running `[load|user]` gives the user-tx-inclusive
  `header.state_root()`, but the existing overwrite `composer.rs:1826-1833` iterates an EMPTY `stateDeltas` vec → does
  nothing). **Q1 (verified):** the outbound L1 entry lives in `composition.targets[L1].batch` (zk-poster leg
  `composition.rs:630`, NOT `source.batch`) → explicit splice, no double-count. **Q2 (Edu):** anchor the LAST OUTBOUND
  entry's delta explicitly (`proxyEntryHash==0` + non-empty `l2ToL1Calls`), not `entries.last()` — future-proof for
  A2b. **Q3 (Edu):** ship the outbound USER tx in the sync-block DA slot (`composer.rs:1971`); the deriver
  reconstructs the load tx via the SHARED `build_outbound_load_table_txs` from the on-chain `entries[]`; `l2_entries`
  stays inbound-only. Sub-steps (each green + inbound byte-identical):
  - **A2.3a** — wire the L2 entry client (A1.2): a 2nd L2 client via `LocalChainClient::new_entry(…EvmL2Style)`
    (Role::Entry, so `simulate_source_tx` works), held `Option<Arc<dyn EntryChainClient>>` on eez-composer `Inner`.
  - **A2.3b** — drain direction-branch (`held.direction`) + R1 poison gate (outbound with 0 L1 entries → poison, NOT
    silent survivor; never reaches `build_inbound_system_txs`). OUTBOUND calls `simulate_and_resolve_recorded_for(L2,
    l2_entry_client, raw_tx)`; builds the outbound entry + load tx + a `SyncPair`.
  - **A2.3c** — `build_sync_block` call-site: `interleave_sync_block_txs(pairs)` (all-inbound == today byte-for-byte).
  - **A2.3d** — `prepare_post_batch_raw`: splice outbound entries after the anchor + CREATE one chained StateDelta
    each (newState = `sync_block_state_root`) + `transientExecutionEntryCount=1+N` + anchor the last-outbound delta (Q2).
  - **A2.3e** — DA transport (outbound user tx in the sync-block DA slot, Q3) + L1-bundle split (inbound survivors →
    L1 bundle; outbound survivors → L2 block only; same for the deferred-arm `survivor_raws`).
- **A2.4 ✅ DONE** (`7d4c4d4`; lockfile `158e8d1`) — deriver `reconcile_batch_blocks`: partition the L1 entries by the
  `proxyEntryHash==0 + non-empty l2ToL1Calls` outbound discriminator (R1), lower each via the SHARED
  `build_l2_outbound_entry` + `build_outbound_load_table_txs` on the SYSTEM_ADDRESS nonce BEFORE the inbound deliveries,
  `consumed_deferred = settled_count - (1 + N_outbound)`. `pub use eez_protocol::RollupId` re-export from eez-evm (no new
  dep). Inbound-only provably byte-identical (empty outbound → unchanged path). Workspace green, eez-evm 89/89,
  eez-composer 30/30, clippy clean. **The composer↔deriver outbound path is now FULLY WIRED on both sides.**
- **P-1 ✅ DONE** (`c69eda2` `75bfaed` `60969c0` `224b9ae`) — full anvil E2E exit assertion GREEN
  (`e2e_outbound.rs` S1 + S2-S5). An L2 outbound `setValue(42)` settles on L1 (`Value.value()==42` +
  `rollups[1].stateRoot == L2 safe-block root`), a fresh FOLLOWER re-derives the user-tx-inclusive settled root from L1
  ALONE (the exact-root match IS the `loadExecutionTable` byte-equality proof), + a negative control. **P-1 found and
  fixed 3 real bugs** the static review/compose path had hidden:
  1. **Outbound entry `destinationRollupId` = rid, not MAINNET(0)** (composer `prepare_post_batch_raw` splice). The
     protocol's canonical L2→L1 entry uses the SOURCE rollup id (`IntegrationTestBridge.t.sol`,
     `EXECUTION_ENTRY_SPEC.md`, `EEZ.sol _validateStructure` — MAINNET(0) can never be a registered member).
     `assert_batch_registry_native` was CORRECT and stays unchanged. (An audit workflow first mis-concluded the gate
     should relax; an adversarial re-verify against the canonical test corrected it.)
  2. **Don't merge the outbound composition's `source.batch`** into the postBatch — the L2 entry chain double-counted
     the call and re-introduced the dest=0 entry. Outbound survivors no longer push to `survivor_comps`.
  3. **L1-derived follower catch-up self-stranded** (`60969c0`) — a PRE-EXISTING port regression (NOT the outbound
     work; proven by an empty-chain isolation test). `catch_up` force-replayed already-correct blocks → backward
     head-FCU → reset → deadlock. Fixed `force_replay=false` + `sealed_header_or_head` (committer in-memory head
     fallback) + safe-FCU fallback-to-finalized.
  Lib 194/194, clippy clean, full e2e_outbound suite green.
- **#2 (non-void returnData) ✅ DONE** (`896cf1c`) — NON-ISSUE in eez: the outbound source-sim runs the L2→L1 call
  against the composer's L1 entry client (L1 IS in the rollup map, `main.rs .entry(...)`), so
  `ExecutedAction.outcome.return_data()` already captures the REAL L1 return; the entry `rollingHash` matches
  `EEZ._processNCalls`. The test now uses the NON-VOID `Value` (returns `(bool,uint256)`) directly — zero production
  change. (The void first cut was a preemptive sidestep adopted alongside the source.batch-merge fix.)
- **A2b (mixed batch) ← NEXT.** Needs INBOUND e2e built first (the harness has NO inbound support: `cross_chain_env`
  only sets `EEZ_CROSS_CHAIN_PROXY_ADDRESSES`, no `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS`, no L1-proxy helper, no inbound
  send). Mirror the outbound test inbound: deploy `Value` on L2, `EEZ.createCrossChainProxy(L2Value, L2_id)` on L1, an
  L1-chain-id tx to the L1 proxy → L2 ingress, assert the L2 `Value` is set + the follower re-derives. THEN mixed: both
  directions in one slot → layout `[anchor | outbound | inbound deferred]` (contract already tolerates it; A2.4 deriver
  partition already splits them). Session-sized.
