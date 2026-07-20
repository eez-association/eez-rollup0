# Local network runbook

Run the full rollup stack on one machine — no Chiado, no docker, no anvil.
One `eez-node` process boots the embedded dev L1 (chainId 1337, 5s
auto-mine, mock `eth_sendBundle`) **and** the L2 (chainId 1, 1s blocks)
plus both cross-chain ingress fronts. This is the same topology the
`cross_chain.rs` e2e test uses (`DevnetCfg` in
`crates/eez-node/tests/common/mod.rs`), driven by hand.

Prereqs: rust toolchain, foundry (`cast`/`forge`), `jq`, `python3`, `tmux`,
and `git submodule update --init --recursive` + `forge build` in `contracts/`.

```bash
scripts/local-net.sh up        # build + boot the composer node (fresh chain)
scripts/local-net.sh deploy    # deploy EEZ + MockPS + Rollup + bridge onto the embedded L1
scripts/local-net.sh wave      # smoke: bidirectional setters + deposit + withdrawal via the fronts
scripts/local-net.sh follower  # second node re-deriving the L2 purely from L1
scripts/local-net.sh status    # heads, batch counts, L1↔L2 state-root reconcile
scripts/local-net.sh down
```

| Endpoint | URL |
|---|---|
| Embedded L1 RPC | `http://127.0.0.1:18545` |
| L2 RPC | `http://127.0.0.1:18688` |
| L1→L2 front (inbound) | `http://127.0.0.1:18999` |
| L2→L1 front (outbound) | `http://127.0.0.1:18998` |
| Follower L2 RPC | `http://127.0.0.1:18788` |

## Why deploy comes *after* boot

Composer mode requires the protocol addresses in env at startup, but the
embedded L1 doesn't exist until the node is up. The loop is broken by
determinism: the deployer key's first three CREATEs pin EEZ / MockPS /
Rollup at known addresses, so `up` starts the node with those baked in and
`deploy` (a thin wrapper over `scripts/deploy.sh`) makes them real. The
initial state root registered on L1 is read from the live L2's block 0.
`deploy` hard-fails if an address lands off-prediction (i.e. the L1 chain
wasn't fresh).

## What `wave` proves

Four user txs are signed against the *source* chain and submitted to the
fronts, which hold them for the next Sync slot instead of forwarding:
an inbound L1→L2 setter, an inbound deposit to a fresh L2 recipient, an
outbound L2→L1 setter, and an outbound withdrawal to a fresh L1 recipient.
Convergence (usually <30s) means: fronts held + composed, source txs
landed atomically with `postBatch`, target-side effects executed, and
value moved across chains in both directions.

## Verifying by hand

```bash
# batches + attestations since deploy
cast logs --rpc-url http://127.0.0.1:18545 --address <EEZ> 'BatchPosted(uint256)' --from-block <deploy>
# stored root must track the L2 safe head
cast call <EEZ> 'rollups(uint256)(address,bytes32,uint256)' 1 --rpc-url http://127.0.0.1:18545
cast block safe --rpc-url http://127.0.0.1:18688 --json | jq .stateRoot
# decode a live batch
cast tx <postBatch-hash> --rpc-url http://127.0.0.1:18545 --json | jq -r .input | sed 's/^0x//' > /tmp/batch_calldata_raw.hex
cargo run -p eez-evm --example decode_batch
```

Negative paths worth trying: a tx to a non-proxy address through a front
is evicted within one slot ("can never compose, resubmit required" in the
log); a stale nonce is rejected at the front with the expected/held
counts; killing `tmux` session `eez-net` and re-running the node command
(without wiping datadirs) resumes from the L1-derived cursor.

## Test protocol

What to run, for how long, and what must hold. Every tier ends with
`scripts/local-net.sh verdict`, which prints a fixed RESULTS block and
exits 0/1 — that block (plus the failing log excerpt, if any) is what you
paste back into the PR/issue.

| Tier | Command(s) | Duration | Matrix | Pass criteria |
|---|---|---|---|---|
| 0 — suites | `cargo test --workspace --exclude eez-node`; `cargo test -p eez-node -- --test-threads=4`; submodule `forge test` | ~10 min | all unit/e2e/Solidity tests | zero failures (a single heavy-e2e timeout under parallel load is a known flake — rerun it isolated before calling it a regression) |
| 1 — smoke | `up`, `deploy`, `wave`, `verdict` | ~5 min | 1 wave: inbound setter + inbound deposit + outbound setter + outbound withdrawal via the fronts | `wave` converges <150s; verdict PASS |
| 2 — soak | tier 1, then repeat `wave` ×3, add 1 poison tx (front-held tx to a non-proxy address) and a few plain L2 transfers | ~15 min | both directions × {setter, value transfer} ×3 + poison + pure-L2 | all wave convergences; poison never mines and logs an eviction; verdict PASS with evictions == poison count |
| 3 — durability | tier 2, plus `follower`, then kill tmux `eez-net` and re-run the node command on the same datadirs | ~30 min | derivation + crash recovery | follower reaches an identical block hash at the common safe height and stays in lockstep; restarted composer resumes from the L1-derived cursor and verdict still PASS |

Verdict criteria (checked automatically): state divergence = 0,
`ImmediateEntrySkipped` = 0, bundle drops = 0, exact L1↔L2 state-root
reconcile, N+1 next-slot bundle hit-rate ≥ 90% (healthy runs sit at
~100%; the target is deliberately below that to absorb slot boundaries).
Evictions are reported but only fail review if they exceed the poison
txs you sent.

Reporting back: paste the VERDICT block verbatim, plus L2/L1 head heights
from `status` and, on any FAIL, the matching lines from
`$EEZ_NET_DIR/composer.log` (grep `diverged|evict|Fatal`). CI can gate on
the exit code alone.

## Gotchas

- **dotenvy walks ancestor directories for `.env`.** A stray file with
  `EEZ_PROOF_SIGNER_KEY` on the node's cwd path silently flips a follower
  into composer mode (crashes on embedded-L1 port collision). Presence is
  what matters — an empty value still flips the mode.
- Genesis must be restamped to wall-clock now on every fresh `up` (the
  sequencer's lateness gate treats a stale genesis as perpetually late);
  restamping doesn't change the state root, so the registered
  `initialState` stays valid.
- `scripts/xchain-test.sh` / `devnet-test.sh` do **not** work here — they
  are hardwired to the dockerized Chiado stack. Drive the fronts with
  plain `cast send` (they proxy all other `eth_*` to their upstream, so
  nonce/chain-id lookups just work).
- The protocol's own Solidity suites live in the `sync-rollups-protocol`
  submodule (`forge test` there; the anvil e2e scenarios take
  `BASE_PORT=28545` to stay clear of this network's ports).
