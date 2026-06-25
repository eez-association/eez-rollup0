# P-1 — Outbound (L2→L1) anvil E2E acceptance test — DESIGN

Status: design complete, implementation starting. Branch `feat/l2-to-l1-crosschain`.
This is A2's **exit assertion**: a fresh follower re-derives the settled,
user-tx-inclusive root for a value-free state-mutating `setValue` outbound call.

## 0. What P-1 proves

The full outbound core (A2.1–A2.4) is wired + green. P-1 proves it END-TO-END:
a real L2-signed outbound `setValue` tx → composer drains it (outbound arm) →
`loadExecutionTable` + the user tx run in the Sync block → `postBatch` settles on
L1 (EEZ executes `setValue` on the L1 target) → a FRESH follower (sequencer +
composer disabled) re-derives the SAME settled root from L1 alone. Plus a
`loadExecutionTable` byte-equality sub-assertion (composer-emit == deriver-rebuild)
and a negative control.

## 1. Topology — the MIRROR of `scripts/devnet-test.sh`

`devnet-test.sh` is the canonical working cross-chain test, but it is **INBOUND**
(Value on L2, proxy on L1, L1-chain-id tx → L1 proxy → submitted to L2 ingress).
P-1 is its exact mirror:

| | INBOUND (devnet-test.sh) | OUTBOUND (P-1) |
|---|---|---|
| `setValue` target | L2 | **L1** |
| CrossChainProxy | L1 (`EEZ.createCrossChainProxy(L2Value, L2_id)`) | **L2** (`EEZL2.createCrossChainProxy(L1Value, MAINNET=0)`) |
| user tx chain-id | L1 | **L2** |
| user tx `to` | L1 proxy | **L2 proxy** |
| classifier signal | `EEZ_CROSS_CHAIN_SOURCE_CHAIN_IDS` (L1 id) | **`EEZ_CROSS_CHAIN_PROXY_ADDRESSES`** (L2 proxy addr) |
| submitted to | L2 ingress (`eth_sendRawTransaction` @ L2 RPC) | L2 ingress (same) |

The L2 proxy lives on EEZL2 (genesis predeploy at `0x4200…0007`). The L1 Value is
the settlement target the EEZ `_processNCalls` calls during `postBatch`.

## 2. The embedded-L1 requirement (the hard part)

The cross-chain `EvmComposer` is built ONLY when `embedded_l1.as_ref()` is `Some`
(`main.rs:449`); its L1 entry-client/root-reader hardcode `l1_handle.node.provider`
(the node's OWN embedded reth, `main.rs:481`). **There is NO path to the cross-chain
composer from an external anvil** — so P-1 MUST run with `EEZ_L1_EMBEDDED=1`. In
embedded mode `EEZ_L1_RPC_URL` points at the embedded L1's own HTTP port
(`main.rs:651-654`), so `postBatch`, the root-reader, and contract deploys ALL go to
the embedded reth.

Production sidesteps deploy-ordering by using **chiado** (embedded reth_gnosis syncs
a real L1 where EEZ is already deployed). A dev/anvil test has no external chain to
sync, and the embedded reth `--dev` boots EMPTY with the node — yet the node requires
`EEZ_REGISTRY_ADDRESS` at startup (`main.rs:458`). Resolution = **placeholder-then-restart**
(supersedes the earlier precompute idea — less fragile, and a restart is needed anyway
for the proxy address):

- **Phase A** — start the node with a PLACEHOLDER `EEZ_REGISTRY_ADDRESS` (any valid
  hex, e.g. `0x00…01`). Line 458 only parses it; the composer then fails to read
  `rollups[]` from a codeless address → no `postBatch`, just retries. No crash. The
  embedded reth `--dev` spawns and exposes HTTP (`l1_embedded.rs:63`, `http: true`).
- Deploy to the embedded L1 (`http://127.0.0.1:EEZ_L1_HTTP_PORT`): EEZ → MockPS →
  Rollup → registerRollup → Value (L1 target), capturing the REAL addresses + deploy
  block. Then on L2: `EEZL2.createCrossChainProxy(L1Value, 0)` → read proxy P.
- **Phase B** — RESTART the node (same L2 datadir + same `EEZ_L1_DATADIR` so the
  embedded L1 keeps EEZ; same pinned `EEZ_L1_HTTP_PORT`) with the REAL
  `EEZ_REGISTRY_ADDRESS` / `EEZ_REGISTRY_DEPLOY_BLOCK` / `EEZ_MOCK_PROOF_SYSTEM_ADDRESS`
  AND `EEZ_CROSS_CHAIN_PROXY_ADDRESSES=P`. Now the composer reads EEZ and the
  classifier tags the outbound tx.
- **Keys.** Deploy the protocol from `EEZ_L1_POSTER_KEY` (anvil#0) — the composer
  only starts posting after EEZ exists, which is what the test deploys, so the
  poster's first tx IS the EEZ deploy (no nonce race in Phase A). If any race
  appears, deploy from a distinct funded key (anvil#1) and keep anvil#0 for the
  composer poster. `EEZ_INITIAL_STATE_ROOT` at RegisterRollup = the cross-chain L2
  genesis state root.

## 3. Required env (beyond the existing `env_for`)

```
EEZ_L1_EMBEDDED=1
EEZ_L1_HTTP_PORT=<free>            # embedded reth http; EEZ_L1_RPC_URL = http://127.0.0.1:<free>
EEZ_L1_AUTH_PORT / EEZ_L1_P2P_PORT=<free>   # avoid the 18545/30444 defaults (parallel tests)
EEZ_L1_DATADIR=<tempdir>          # embedded reth datadir
EEZ_L1_RPC_URL=http://127.0.0.1:<EEZ_L1_HTTP_PORT>
EEZ_L1_CHAIN_ID=1337              # reth --dev
EEZ_CCM_L2_ADDRESS=0x4200000000000000000000000000000000000007   # EEZL2 genesis predeploy
EEZ_L2_SYSTEM_KEY=<anvil#0 priv>  # SYSTEM_ADDRESS signer for loadExecutionTable
EEZ_L2_SYSTEM_ADDRESS=0xf39Fd6…2266
EEZ_REGISTRY_ADDRESS=<precomputed CREATE(deployer,0)>
EEZ_MOCK_PROOF_SYSTEM_ADDRESS=<precomputed CREATE(deployer,1)>
EEZ_ROLLUP_ID=1
EEZ_CROSS_CHAIN_PROXY_ADDRESSES=<L2 proxy>    # Phase B only
# L2 genesis = the cross-chain fixture (EEZL2 at 0x4200…0007): NodeConfig.genesis_path
```

The L2 genesis MUST be the cross-chain-capable one. `crates/eez-node/tests/fixtures/genesis.json`
already predeploys EEZL2 at `0x4200…0007` (codelen ~24390) — reuse it (the reorg test
uses it via `reorg_genesis_path()`), or bake a dedicated outbound fixture. `EEZ_INITIAL_STATE_ROOT`
at RegisterRollup must equal that genesis' state root (`reorg_genesis_state_root()`).

## 4. Test sequence

1. Harness: alloc free L1 ports, tempdir L1 datadir, build the cross-chain env.
2. Start node (Phase A, embedded L1 + cross-chain env, cross-chain L2 genesis, NO proxy env).
3. Wait for the embedded L1 RPC (http://127.0.0.1:EEZ_L1_HTTP_PORT) up.
4. Deploy to the embedded L1 from the deployer key (anvil#1): EEZ (nonce0), MockPS (nonce1),
   Rollup (nonce2), registerRollup(initialState = cross-chain-genesis state root), Value (L1 target).
   Assert EEZ landed at the precomputed address.
5. On L2: `EEZL2.createCrossChainProxy(L1Value, 0)` → read proxy address P.
6. Restart node (Phase B) with `EEZ_CROSS_CHAIN_PROXY_ADDRESSES=P`.
7. Send the outbound user tx: L2-chain-id signed, `to=P`, data=`setValue(42)`, to the L2 RPC.
8. Wait for ≥1 `postBatch` on the embedded L1 + the user-tx L1 receipt; assert L1
   `Value.value()==42` (settlement executed) AND `rollups[1].stateRoot == L2 head root`.
9. Spawn a FRESH follower (sequencer+composer disabled, same cross-chain L2 genesis +
   the L1 watcher env) → `wait_for_node_caught_up` → it re-derives the settled root.
10. Sub-assertions: (a) the deriver's reconstructed `loadExecutionTable` system tx ==
    the composer's emitted one (byte-equality — log scrape or fixture); (b) negative
    control: a non-proxy L2 tx is NOT classified outbound (no spurious entry).

## 5. Build order (incremental, each committed, inbound byte-identical)

- **S1** — harness: cross-chain embedded-L1 bring-up + env builder + precompute helpers;
  prove the node comes up + settles (empty batch ok) against the embedded L1.
- **S2** — deploy EEZ/PS/Rollup/Value to the embedded L1 + create the L2 proxy + restart.
- **S3** — outbound user tx → settle → L1 `Value.value()==42` + root reconciliation.
- **S4** — fresh-follower re-derivation assertion (the core P-1).
- **S5** — loadExecutionTable byte-equality + negative control.

## 6. Notes / risks

- The existing e2e harness (`common/mod.rs`, 2026-06-11) PRE-DATES the cross-chain
  composer (a54181a, 2026-06-12) and its `happy_case` FAILS today (composer needs
  embedded L1 + `EEZ_CCM_L2_ADDRESS`, never wired). P-1's harness work makes the
  cross-chain bring-up a first-class harness capability; the existing non-cross-chain
  tests can keep `EEZ_L1_EMBEDDED=0` (restoring their original empty-batch behavior).
- `Address::create(deployer, nonce)` (alloy) computes the CREATE address for the
  precompute. Verify the alloy API name at implementation time.
- reth `--dev` embedded auto-mines; `EEZ_L1_POSTBATCH_PRIORITY_FEE=10gwei` orders
  postBatch ahead of the 2-gwei user tx within an L1 block (main.rs:688-695).
