# Out-of-process prover chain + Chiado deployment — progress & next steps

**Branch:** `feat/port-based-crosschain-core`  ·  **Last updated:** 2026-06-19

This tracks the port of the out-of-process ZisK prover loop into eez-rollup0 and
its deployment against the Gnosis **Chiado** testnet (the real L1). The prover
chain LOGIC is complete and validated live end-to-end; the remaining work is the
deployment integration (the composer's deferred post + the real on-chain
verifier + running the stack on Chiado).

---

## 1. The architecture (the loop)

```
composer ──build settling batch──▶ control feed (control.v1.PostBatch)          P4-a
   eez-proverd ──subscribe──────────▶ native-validate (ZisK stateless re-exec)    P3 step1
              ──gate──────────────────▶ publicInputsHash recompute                 P3 step2a
              ──gate──────────────────▶ endpoint + parent-anchor OD-5 + telescoping
                                        + single-rollup + interior + system-tx #10  P3 step2b
              ──sign──────────────────▶ ECDSA sign(publicInputsHash) → 65B r||s||v  P3 step3
              ──ProofSink.SubmitSlotProof──▶ SlotProof                              P3 step3
composer ◀─verify (ecrecover == attester)── ProofSink                              P4-b
         ──apply_proof──▶ batch.proofs[] = [sig] ──finalize──▶ post to L1          P4-b-full ✅ (live on Chiado)
   on-chain ECDSAProofSystem.verify(proof, publicInputsHash) == signer ──▶ settled  ✅ status 1
```

- **Composer side** = `crates/eez-composer` + `crates/eez-node` (the `eez-node`
  binary, composer mode). Hosts `control.v1.ControlFeed` + `control.v1.ProofSink`
  on `127.0.0.1:${EEZ_CONTROL_RPC_PORT:-50051}`.
- **Prover side** = `crates/eez-proverd` (the out-of-process daemon) + the ZisK
  `native-validate` binary at `/home/ubuntu/zisk-eth-client/target/release/native-validate`.
- **vkey convention:** `vkey = bytes32(uint160(attester_address))`. The attester =
  `EEZ_PROOF_SIGNER_KEY`'s address. `eez-proverd` derives the gate vkey from
  `--signer-key` automatically; the rollup is registered with that vkey, and the
  real `ECDSAProofSystem.signer` is that address. **These three must agree.**

---

## 2. What's DONE (validated)

The prover chain is logically COMPLETE and validated live against a real captured
settling fixture (`crates/eez-proverd/tests/fixtures/{block-13.rlp,witness-13.json,
postbatch-13.json}`).

| Piece | Commits | Validated |
|---|---|---|
| P4-a composition feed | `47e4490` `fa17348` `c121fb9` | compile |
| P3 step1 window aggregation | `4b10dc2` | live (feed_fixture) |
| P3 step2a publicInputsHash gate | `a8d03ae` | unit + real fixture |
| review-driven fixes (prover-feed bugs) | `7373d0b` `a0e5baa` | live e2e (settling blocks reach prover, sink bounded) |
| P3 step2b-2 settlement-chain gate | `f51dea9` `d28ec05` | live (all gates) |
| P3 step2b-3 interior + reverted-system-tx #10 | `9b6f3f3` | live (full suite) + 14/14 unit |
| P3 step3 ECDSA attestation + ProofSink submit | `d71e512` | live (✓ ATTESTED) |
| P4-b composer ProofSink (verify the attestation) | `bff31c4` | live (✓ verified) + 3/3 unit |
| P4-b-full deferred-post primitives (ProofStore + apply_proof) | `27a448d` | unit (full data-flow) |

**Live validation recipe** (the whole loop, against the embedded fixture):
```bash
cd /home/ubuntu/eez-rollup0
cargo build -p eez-node --example feed_fixture -p eez-proverd
NV=/home/ubuntu/zisk-eth-client/target/release/native-validate
CFG=/tmp/zv2/chainconfig.json   # complete alloy ChainConfig, ALL forks at 0
SIGNER=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80  # hardhat #0
RUST_LOG=info ./target/debug/examples/feed_fixture &   # ControlFeed(block-13) + real ProofSink
./target/debug/eez-proverd --control-addr http://127.0.0.1:50051 \
  --validator-bin $NV --chain-config $CFG --signer-key $SIGNER --max-window 1
# expect: ✓ native-validate accepted window + ✓ publicInputsHash + ✓ settlement chain
#         + ✓ ATTESTED, and composer-side ✓ ProofSink: verified attestation
```

---

## 3. The Chiado deployment replan (in progress)

Chiado as the SINGLE L1 dissolves the local e2e's two-L1 mismatch: `eez-node`
embeds a Chiado L1 (`EEZ_L1_EMBEDDED=1`, chain-id 10200, reth_gnosis) tracking
canonical Chiado, and the composer uses THAT as its L1. **Infra is ready** (L1
snapshot in `data/chiado-l1`, JWT, `configs/chiado`, `eez-node:local` image,
`native-validate`, `.env.chiado` funded keys, the real `ECDSAProofSystem`
compiles). Crux: today the deploy registers the MOCK PS (fixed digest); the REAL
`sync-rollups-protocol/src/proofSystems/ECDSAProofSystem.sol` recovers over the
actual `publicInputsHash` — switching to it REQUIRES the deferred post.

### Phase 1 — contracts ✅ (`b58cebe`)
- `contracts/script/DeployECDSAProofSystem.s.sol` — deploys the submodule's real
  PS (`run(owner, signer)`); imports via the existing `sync-rollups-protocol/`
  remapping + OZ from submodule `libs` (no copy). Compiles clean.
- `scripts/deploy.sh` — `EEZ_PROOF_SYSTEM=mock|real` (default mock, additive).
  `real` deploys the ECDSAProofSystem + registers the rollup with it; writes
  `EEZ_PROOF_SYSTEM_ADDRESS` + `EEZ_PROOF_SYSTEM_KIND` to `deployments.env`.

### Phase 2 — composer deferred post (the central refactor)
- **step 2a ✅ (`afcab2c`)** — extracted `Composer::finalize_post_batch_tx(batch, ctx) -> raw_tx`
  (encode + sign). Pure refactor, mock UNCHANGED, 27/27 composer tests green.
- **step 2b-1 ✅ (`a05087a`)** — `Composer` gained `proof_store` (OnceLock) +
  `set_proof_store` + `deferred_post()` (presence = deferred mode); `eez-node`
  creates the `ProofStore` ONLY when `EEZ_PROOF_SYSTEM_KIND=real` and shares it
  with the composer + the ProofSink (`ProofSinkSvc::with_store`). Additive.
- **step 2b-2/2b-3 ✅ (`3a7dca3`)** — the deferred-dispatch (§4). Added
  `PostBatchOutcome {Ready|Deferred}`; `prepare_post_batch_raw` returns it
  (recomputes the publicInputsHash + returns `Deferred` WITHOUT signing when
  `deferred_post()`, fail-closed on hash error; otherwise `Ready` via the
  `finalize_post_batch_tx` seam). New `spawn_deferred_post` polls the ProofStore
  ~30s for the attestation → `batch.proofs[] = [sig]` → finalize → optimistic
  dispatch. Both callers (`compose_via_evm_composer`, `dispatch_minimal_postbatch`)
  match on the outcome. Mock path byte-unchanged. 27/27 composer tests green,
  clippy clean, eez-node builds. **Validate live at Phase 4 (Chiado, real PS).**

### Phase 3 — run eez-proverd against the composer ✅ (`ef00970`)
Validated LIVE against the real Chiado composer (2026-06-19). Run on the host
(host-networking → `127.0.0.1:50051`):
```
SIGNER=$(grep '^EEZ_PROOF_SIGNER_KEY=' .env | cut -d= -f2)
RUST_LOG=info,eez_proverd=debug ./target/release/eez-proverd \
  --control-addr http://127.0.0.1:50051 \
  --validator-bin /home/ubuntu/zisk-eth-client/target/release/native-validate \
  --chain-config configs/l2-chainconfig.json \
  --signer-key $SIGNER
```
Observed loop: `subscribed from_block=1` → one-time `DATA_LOSS` (ring eviction,
resyncs to the live tip — expected) → `✓ native-validate accepted window` per
live window → on settling windows `✓ publicInputsHash recomputed = composer
claim` + `✓ settlement chain telescopes` + `✓ ATTESTED`; composer-side `✓
ProofSink: verified attestation from the registered prover` (attester
`0xfB05…`). Steady-state attestation per settling slot.

**Two infra gaps fixed to get here (commit `ef00970`):** (1) the Dockerfile was
missing `protobuf-compiler` (the tonic crates run `tonic-prost-build` in
`build.rs`); without it the image build dies at `cargo build -p eez-node`
(exit 101). Added AFTER the cook layer (cook stubs workspace members) to keep
the dep cache warm. (2) `configs/l2-chainconfig.json` (chainId **1**, all forks
at 0 — the L2 id, NOT the L1's 10200). Also: `.env.chiado` needed the real
funded keys (it shipped `CHANGE_ME` stubs) and is now gitignored.

**The `eez-node:local` image must be rebuilt from HEAD** before bring-up — the
chiado compose uses the BAKED image (no dev-overlay), and the pre-existing image
predated the control-feed/ProofSink wiring (`b4ccd3e`). `docker build -t
eez-node:local .` then `docker compose --env-file .env.chiado -f
docker-compose.chiado-node.yml up -d`.

(native-validate's `--dir` JSON includes `pair_roots` + `tx_statuses` for the
interior / system-tx gates. Current deploy is MOCK, so the composer posts
SYNCHRONOUSLY and the prover's attestation is verified-but-not-consumed; the
deferred-post + real on-chain settlement is Phase 4.)

### Phase 4 — validate on Chiado ✅ (2026-06-19)
**DONE — real on-chain settlement validated live on Chiado.** `EEZ_PROOF_SYSTEM=real
EEZ_DEPLOY_SKIP_SIMULATION=1 make deploy-protocol` → registry `0x85f7…`, real
`ECDSAProofSystem` `0x002ca3…` (signer `0xfB05…` confirmed via on-chain
`signer()`), rollupId 1, `RegisterRollup(stateRoot=0xdd37fe70…)`. Wiped the stale
`data/eez-l2` (the running mock chain had genesis `0x5aab3b1d…`; the fresh deploy
re-baked genesis is `0xdd37fe70…` — derived empirically via a throwaway `reth
node` + `cast block 0`, matches deploy.sh's default + the registration). Brought
up the stack → `deployments.env`'s `EEZ_PROOF_SYSTEM_KIND=real` puts the composer
in **deferred mode**; the new genesis ts is recent so L2 catch-up is seconds.

**The deferred-post loop ran end-to-end:** `minimal postBatch deferred; awaiting
prover attestation` → eez-proverd `native-validate accepted window` + `✓
publicInputsHash recomputed = composer claim` + `✓ settlement chain telescopes`
+ `✓ ATTESTED` → composer `✓ ProofSink: verified attestation` → `deferred post:
prover attestation applied, dispatching bundle` → `bundle outcome … settled=true
… state_applied: true`. **On-chain receipt** of a settling postBatch (`0x9178…`,
Chiado block 21709834, to the new registry) = **`status 1 (success)`** — the
**REAL `ECDSAProofSystem.verify` recovered the publicInputsHash and matched the
signer**. Steady-state: a settlement per slot (10+ settled on-chain), with the
expected 1 timeout on an early pre-prover-cursor block (`deferred post timed out
… L1 unadvanced this slot` — the fail-safe, correct). KEY consistency held:
real-PS `signer()` == `EEZ_PROOF_SIGNER_KEY` address `0xfB05…` == prover attester
== rollup `vkey=bytes32(uint160(0xfB05…))`.

So the **whole out-of-process prover chain is validated against the real
on-chain verifier on Chiado** — the deferred-post (Phase 2 step 2b-2/2b-3) is
exercised live, not just compiled.

### C2 ship-gate — validator-mode tamper-refusal ✅ (`c792785`)
The "prover done" criterion, now a repeatable script:
`bash scripts/soundness-tamper-refusal.sh`. Drives the real loop with the
embedded settling fixture (eez-proverd/tests/fixtures, block 13) via
`feed_fixture` + host `native-validate` — no Chiado needed. Asserts: clean
witness → `✓ ATTESTED`; one nibble flipped in the longest `state[]` MPT node →
`native-validate` rejects → prover REFUSES (no attestation). Skips (exit 0) when
`native-validate` is absent. Both cases PASS.

---

## 4. The deferred-dispatch (Phase 2 step 2b-2/2b-3) — ✅ DONE (`3a7dca3`)

**STATUS: implemented as described below (commit `3a7dca3`).** Kept as the
reference for the consensus-critical shape. The only deviation from the sketch:
the deferred branch fails CLOSED on a publicInputsHash recompute error (returns
`Err` → caller degrades) instead of `unwrap_or_default()` — a zero hash would
never match the prover's attestation. NEXT = Phase 3 (run eez-proverd against
the composer on Chiado), then Phase 4 (validate the real settlement on-chain).

The seams are ALL in place (`finalize_post_batch_tx`, `proof_store`,
`ProofSinkSvc::with_store`, `apply_proof`). This is a careful consensus-critical
change to `prepare_post_batch_raw` + its 2 callers; do it FRESH, compile +
`cargo nextest run -p eez-composer` after, validate end-to-end at Phase 4.

**(1) Return enum + the deferred branch.** In `crates/eez-composer/src/composer.rs`:
```rust
enum PostBatchOutcome {
    Ready(Bytes),                                              // synchronous (mock)
    Deferred { batch: Box<eez_evm::EvmBatch>, public_inputs_hash: B256 },
}
```
Change `prepare_post_batch_raw` (`composer.rs:1445`) return `Result<Bytes,String>`
→ `Result<PostBatchOutcome,String>`. At its END (currently
`self.finalize_post_batch_tx(&batch, ctx).await`):
```rust
if self.deferred_post() {
    let h = eez_evm::public_inputs::public_inputs_hashes(&batch, self.inner.prover.vkey(), None)
        .ok().and_then(|v| v.first().copied()).unwrap_or_default();
    Ok(PostBatchOutcome::Deferred { batch: Box::new(batch), public_inputs_hash: h })
} else {
    Ok(PostBatchOutcome::Ready(self.finalize_post_batch_tx(&batch, ctx).await?))
}
```
KEEP the mock `prove()` / proofs-set as-is — the mock sig is harmlessly overwritten
by `apply_proof`, and the hash ignores `proofs[]`. (Optional: skip `prove()` when
`self.deferred_post()` to save a no-op signature.)

**(2) The deferred-dispatch task** (new `Composer` method):
```rust
fn spawn_deferred_post(
    &self, rollup_id: u64, sync_height: u64, mut batch: eez_evm::EvmBatch,
    public_inputs_hash: B256, survivors: Vec<crate::held_pool::HeldTx>,
    parent_header: SealedHeader<alloy_consensus::Header>,
    expected_final_state: B256, optimistic: Arc<OptimisticallyIncluded>,
) {
    let this = self.clone(); // Composer is Clone (Arc<Inner>)
    let store = self.inner.proof_store.get().cloned();
    tokio::spawn(async move {
        let Some(store) = store else { return; };
        // poll up to ~30s for the attestation (settling proof window ~2s on chiado)
        let mut sig = None;
        for _ in 0..150 {
            if let Some(s) = store.lock().ok().and_then(|mut m| m.remove(&public_inputs_hash)) {
                sig = Some(s); break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        let Some(sig) = sig else { /* error!("deferred post timed out") */ return; };
        batch.inner.proofs = vec![sig];                       // = proof_sink::apply_proof
        let Some(ctx) = this.inner.cc_exec_ctx.clone() else { return; };
        match this.finalize_post_batch_tx(&batch, ctx.as_ref()).await {
            Ok(raw) => {
                let post_batch_hash = alloy_primitives::keccak256(&raw);
                let mut bundle = vec![raw];
                bundle.extend(survivors.iter().map(|h| h.raw_tx.clone()));
                optimistic.begin(sync_height, post_batch_hash, parent_header, survivors);
                this.spawn_bundle_observer(ctx.as_ref(), rollup_id, sync_height, bundle,
                                           expected_final_state, optimistic);
            }
            Err(_e) => { /* error! */ }
        }
    });
}
```

**(3) The 2 callers** — `compose_via_evm_composer` (`~974`, the rich dispatch at
`~1250-1287`) and `dispatch_minimal_postbatch` (`~1319`). Replace
`let postbatch_raw = ...; <synchronous dispatch>` with:
```rust
match outcome {
    PostBatchOutcome::Ready(postbatch_raw) => { /* the current synchronous dispatch, unchanged */ }
    PostBatchOutcome::Deferred { batch, public_inputs_hash } => {
        self.spawn_deferred_post(rollup_id, built.header.number(), *batch, public_inputs_hash,
            survivors, parent_header.clone(), built.header.state_root(),
            Arc::clone(&rollup.optimistic));
    }
}
// then return Ok(Some(SyncSlotBlock { payload: built.payload, header: built.header }))
```

Notes: `spawn_bundle_observer(&CrossChainExecCtx, …)` → pass `ctx.as_ref()`;
`HeldTx = crate::held_pool::HeldTx { raw_tx: Bytes, … }`; the `Err` fallback in the
callers already degrades to `dispatch_minimal_postbatch` — keep that.

---

## 5. Key facts / paths

- ZisK validator: `/home/ubuntu/zisk-eth-client/target/release/native-validate`
  (CLI: `native-validate <chainconfig.json> --dir <dir>` over a window;
  `<chainconfig.json> <block.rlp> <witness.json>` single). Guardrail: don't touch
  its CLI or `crates/eez-public-inputs`; never git pull/push the zisk repos.
- chain-config JSON: a COMPLETE alloy `ChainConfig` with ALL forks at 0 (the
  minimal `.config` from dump-genesis is insufficient → `WithdrawalsRootUnexpected`).
  Example at `/tmp/zv2/chainconfig.json`.
- Chiado deploy: `EEZ_PROOF_SYSTEM=real make deploy-protocol` (reads `.env`, writes
  `deployments.env`). Run flow in README §"Run a chiado L2 (Docker)".
  `docker-compose.chiado-node.yml` = eez-node(embedded chiado L1 + L2 + composer) +
  lighthouse, host networking. `EEZ_DEPLOY_SKIP_SIMULATION=1` for flaky public RPCs.
