# Composer Fuzzing — Endgame

The composer's decision logic is exercised end-to-end through one entry point,
against the **real EVM + real contract bytecode**. We fuzz `raw_tx` and assert the
produced composition executes + ratifies. reth-node/RPC plumbing is replaced by an
in-process provider; the EVM and contracts are never abstracted.

## Function under test

`eez_protocol::compose_transaction(protocol, entry_client, raw_tx, entry_id, rollups)`
— `eez-protocol/src/compose.rs:54`. Fixed instantiation: `P = EvmProtocol`,
clients = `LocalChainClient`. Under that, it deterministically drives
`LocalChainClient::simulate_source_tx` → real revm → `SessionInspector::call`
(`eez-evm-inspector/src/inspector.rs:489`) — the overlay push/pop pairing + diff-apply.

## Input

- **`raw_tx`**: a signed source tx (the fuzzed variable). Structure-aware generator —
  must call a registered proxy so the cross-chain path fires; random bytes mostly no-op.
- **world**: fixed fixture (below).

## Instrumentation (the fixture)

`simulate_source_tx` reads only **head header + head state** from the provider
(`client.rs:380-440`: `best_block_number`, `header_by_number(latest)`, `.latest()`).
So each chain needs a single-block provider whose state is the deployed world.

Recipe (per chain, L1 entry + L2 target):
1. **Deploy in revm** — EEZL2/`CrossChainProxy` carry constructor immutables
   (`ROLLUP_ID`/`SYSTEM_ADDRESS`, `EEZ`/`ORIGINAL_*`), so the world must be deployed by
   running creation bytecode in a revm `CacheDB`, not hand-inserted. Use `contracts/out/*`
   artifacts: EEZ + MockProofSystem + Rollup + registerRollup + createCrossChainProxy on L1;
   EEZL2/CCM + target contract (`Value.sol`) + proxy on L2.
2. **Snapshot** each touched account (code+storage+balance+nonce) into a
   `reth_provider::test_utils::MockEthProvider` via `ExtendedAccount`; attach one head header.
3. Build `LocalChainClient::new_entry`(L1) + `new_follower`(L2) over those providers
   (template: `eez-node/src/main.rs:432/493`). Slot helpers exposed: `eez_evm::proxy_mapping_key`,
   `CCM_AUTHORIZED_PROXIES_SLOT`, `action::compute_state_root_slot`.
4. Assemble `rollups: HashMap<RollupId, Rollup<EvmProtocol>>` (client + `session: None` +
   `EvmTargetConfig` + `initial_state_root`) — mirror `simulate_and_resolve` Phase 1
   (`composer.rs:525-556`).

## Assertion (oracle = real contracts)

Execute the returned `Composition` against the same bytecode:
- target loads → `EEZL2.loadExecutionTable` + the user tx → **no revert, rolling hash matches**;
- source batch → `EEZ.postAndVerifyBatch` → **ratifies** (emits `L2ExecutionPerformed`).
A broken overlay pairing makes the source sim record wrong inner state/return-data →
surfaces here as revert or rolling-hash mismatch.

## Milestones

1. Fixture plumbing: world-deploy helper + `MockEthProvider` snapshot + `LocalChainClient` build.
2. Green single-hop: one `raw_tx`, source→one target, assert execute+ratify.
3. Nested scenario (target calls back out) → exercises the LIFO pre/post pairing for real.
4. Wrap `raw_tx` in `proptest`/`cargo-fuzz` with the structure-aware generator.

## Discipline

Fuzz `raw_tx` only; the world is fixed. To reach overlay edge-states the world must contain
nested cross-chain contracts and the generator must target their proxies — else the path runs
but trivially (empty stacks).

## Endgame (SOTA notes)

Fixed-world / single-`raw_tx` caps depth at 1. The real target fuzzes the **world itself** as a
sequence of ops (`deploy` / `register_proxy` / `user_tx`) over persistent state.

- **Reference impl: ItyFuzz** (Rust + revm + LibAFL) — our exact stack. Adopt its corpus of
  `(state, single-tx)` pairs + "infant state corpus": snapshot the deepest *valid* state and mutate
  one tx forward, so depth is additive, not re-derived from genesis. arXiv:2306.17135.
- **Restrict the address space (the key change).** Fire condition is `authorizedProxies[target] != 0`;
  a random `target` never hits. The `arbitrary` impl must `choose` the proxy from a runtime
  dictionary (built once via `OnceCell`), not decode 20 raw bytes. Recurse: the inner
  `executeOnBehalf(address, bytes)` payload must also be `selector‖args` drawn from the dict.
- **256-bit `EQ` gives no coverage gradient** (stock CmpLog misses partial-word matches). So the
  dictionary is load-bearing, not optional — never expect feedback to discover an address byte-by-byte.
  For inner `require(x == MAGIC)` walls, record per-PC `abs(Lhs-Rhs)` over `EQ/LT/GT/SUB` in
  `Inspector::step` and harvest operands into the dict (ItyFuzz comparison waypoints / RedQueen).
- **Close deploy→register→nest** via EF/CF-style address *propagation*: capture each
  `createCrossChainProxy` return into the live dict so a later op can reference it. arXiv:2304.06341.
- **Depth objective.** Feed the inspector's own counters (`proxy_lookups`, `recorded_count`,
  `frame_starts` LIFO depth) as a maximization objective = deepest *ratifying* dispatch (oracle-gated),
  plus overlay push/pop imbalance as the bug oracle. Edge coverage alone is flat across depth.

## Status (tests/compose_e2e.rs)

Implemented + green (`cargo test -p eez-composer --test compose_e2e`):
- World boot (L1 EEZ+Rollup+proxy+SetterWrapper / L2 Value+EEZL2), frozen providers, production
  `LocalChainClient`s; `compose_transaction` driven against them.
- **Structure-aware generator** (`FuzzTx`): `Arbitrary` over dictionary indices
  (trigger/method/signer) + typed leaves — address space restricted by construction.
  `CallSpec.payable` gates `msg.value` (non-payable triggers must get value 0, else `EmptyCalls`).
- **Execute+ratify+SETTLE oracle** (`assert_executes_and_ratifies`): replays the composition's own L2
  payloads against the frozen bytecode — `executeIncomingCrossChainCall` checks rolling hash +
  overlay pairing, so a no-revert ratifies the target. CRUCIALLY it then reads `Value@L2` slot-0
  storage back and asserts it equals the value the generator INTENDED to set. The composer's return
  data / rolling hash can look right past a mock prover while the destination never changed — settled
  storage is the ground truth. The predicted value comes from the generator's own `set(x) -> x` model
  (`resolve_and_sign` returns `(raw_tx, expected)`), so the oracle is not circular with the composer.
  Verified: settling really mutates the target (negative control expect-43/settle-42 fails as designed).
  L1 source is a *structural* check only: the composer emits the batch UNSIGNED and `EEZ.sol:435`
  reverts `InvalidProofSystemConfig` on it (proof-signing is downstream; `SIGNER` has no key here).
- **Determinism** (`compose_is_deterministic`): identical input → identical payloads (catches leaked
  `HashMap` order). Dedicated test, not in the hot fuzz loop.

Findings / open edges (hit while building depth-2 nesting — `compose_nested_depth2_ratifies`, `#[ignore]`):
- Composer rejects **same-rollup reentry** (`InvalidReentry`, `composition.rs:734`) — nesting must be
  cross-rollup.
- Cross-rollup L2→**entry**-rollup nesting then fails in the entry-overlay diff-apply:
  `"SELFDESTRUCT mutation at 0x0: out of scope for overlay diff-apply"` — our contracts never
  SELFDESTRUCT, so this is a real composer overlay edge worth a focused look.
- Next step to make depth-2 green: target a **3rd non-entry rollup** (`RollupId(2)` follower) to avoid
  the entry-overlay path.

Coverage-guided fuzzing (`crates/eez-fuzz`):
- Harness extracted to the `eez-fuzz` lib crate (World + generator + oracle, all `pub`) so both the
  integration tests and the `cargo-fuzz` target share one boot.
- `crates/eez-fuzz/fuzz` is the libFuzzer target (`compose`): arbitrary-decode → `FuzzTx` →
  dictionary-restricted `raw_tx` → `compose` → `assert_executes_and_ratifies`. World + multi-thread
  tokio runtime boot once via `OnceLock`. It's a root-workspace MEMBER (shares `Cargo.lock`; a
  separate workspace re-resolves and pulls duplicate `alloy-eip7928`/`revm-state` that break
  `reth-storage-api`) but excluded from `default-members` so normal builds skip it.
- Run: `cd crates/eez-fuzz && cargo +nightly fuzz run compose --sanitizer none`
  (`--sanitizer none` keeps libFuzzer coverage without ASan-instrumenting the whole reth tree).
  Verified: ~1,400 exec/s, coverage-guided corpus growth, no crashes on the single-hop world.

Not yet done:
- Depth-2 nesting green (needs the 3rd-rollup target — see above).
- Seed the libFuzzer dictionary with live proxy addresses + selectors (the `arbitrary` indexing
  already restricts top-level targets; a `.dict` helps addresses buried in inner calldata).
