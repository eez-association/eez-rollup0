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
