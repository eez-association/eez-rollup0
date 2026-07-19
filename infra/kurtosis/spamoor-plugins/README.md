# eez-rollup spamoor plugin

A [spamoor](https://github.com/ethpandaops/spamoor) plugin (Yaegi-interpreted,
no build step) providing the `eez-xchain` scenario: continuous inbound
(L1→L2) or outbound (L2→L1) cross-chain load against the EEZ devnet, run
alongside the existing L1-only `eoatx` baseline. Inbound and outbound run in
separate Spamoor daemons so their clients, roots, nonces, funding batchers,
wallet pools, and lifecycles cannot interfere.

Each spammer cycles through one or more **op kinds** (the `ops` config),
mirroring the operations `wave-test.sh` fires:

| op | call | target |
|---|---|---|
| `set` (default) | `setValue(uint256)` | setter `CrossChainProxy` |
| `noret` | `setValue(uint256)` | `ValueNoRet` `CrossChainProxy` |
| `value` | plain value transfer | recipient proxy (deposit inbound / withdraw outbound) |
| `wrapper` | `setViaProxy(uint256)` | wrapper contract over the setter proxy |

Omit `ops` for the original setter-only load; set `ops: [set, noret, value,
wrapper]` to exercise the full cross-chain surface. Two `attack` modes
(`garbage-calldata`, `revert`) send malformed traffic for DDoS-resilience
testing.

## Quick start (recommended): `spammers.sh`

You declare which spammers you want in an intent file; the orchestrator does
everything else — provisions the proxies/wrappers, funds the outbound daemon
root, injects targets per spammer, and routes each entry to the correct daemon
API. You never copy a proxy address or private key.

```bash
# 1. bring the enclave up (once):
bash infra/kurtosis/up.sh

# 2. first time only: create and edit the workload config:
cp infra/kurtosis/spamoor-plugins/spammers.example.yaml \
  infra/kurtosis/spamoor-plugins/spammers.yaml

# 3. provision + start every enabled spammer:
bash infra/kurtosis/scripts/spammers.sh up

# inspect / sanity-check / apply config edits / tear down:
bash infra/kurtosis/scripts/spammers.sh status
bash infra/kurtosis/scripts/spammers.sh verify
bash infra/kurtosis/scripts/spammers.sh restart
bash infra/kurtosis/scripts/spammers.sh down
```

Requires `kurtosis`, `curl`, `jq`, `cast`, and `python3` with `pyyaml`
(`pip install pyyaml`) on the host. The rest of this doc covers the manual path
and the config reference the orchestrator fills in for you.

Runs as `spamoor-eez-inbound` (normal L1 RPC) and `spamoor-eez-outbound`
(normal L2 RPC). Each has an independent web UI:

```bash
kurtosis port print eez-devnet spamoor-eez-inbound http
kurtosis port print eez-devnet spamoor-eez-outbound http
```

Throughput remains adjustable live without restarting the enclave.

## Prerequisites: this scenario does not provision proxies

`eez-xchain` only drives load against **already-created** cross-chain
proxies — it doesn't deploy targets or create proxies itself. Create them the
same way `infra/kurtosis/scripts/wave-test.sh` does:

- Inbound proxy: `create_l1_proxy <L2 Value target>` — a `CrossChainProxy` on
  the shared L1, created via `createCrossChainProxy(registry, target, rollupId)`.
- Outbound proxy: `create_l2_proxy <L1 Value target>` — created via
  `computeCrossChainProxyAddress` + `createCrossChainProxy` on the L2 CCM
  predeploy (`EEZ_CCM_L2_ADDRESS`).

Easiest path: don't do this by hand — `spammers.sh up` runs
`infra/kurtosis/scripts/xchain-provision.sh`, which creates the full set
(setter, noret, deposit/withdraw, wrapper per direction) idempotently, funds
the configured outbound daemon root, and caches the public addresses to
`datadir/xchain-provision.env`. Only reach for the manual steps above if you're
wiring startup configs by hand.

## Adding the cross-chain spammer

The daemon boots with **no** startup spammers by default — the cross-chain
scenario needs proxy addresses that don't exist until after deploy. Once you
have real proxy addresses, use `spammers.sh up` or the matching daemon UI.
For baked-in configuration, copy the inbound/outbound startup example to its
matching gitignored file and set `inbound_startup_spammer_config` or
`outbound_startup_spammer_config`. Never load an inbound entry in the outbound
daemon or vice versa.

### Recommended: two independent spammers (inbound + outbound)

Inbound and outbound are **separate spammers in separate daemons**. This gives
each direction its own live throughput dial (throttle one without touching the other) and isolates
failures (an outbound-front stall can't starve the inbound side's pending-tx
budget). Enable both entries to run both directions. Each entry can run every
transaction type through `ops: [set, noret, value, wrapper]`; `mode` only
selects the source chain.
This also matches spamoor's own idiom (its built-in
"regular chain load" is many single-purpose spammers, not one mega-scenario).

```yaml
- scenario: eez-xchain
  name: "EEZ Inbound (L1->L2)"
  config:
    mode: inbound
    throughput: 20            # independent dial — tune live in the UI
    max_pending: 200
    max_wallets: 40
    inbound_proxy: "0x..."    # from create_l1_proxy (wave-test.sh)

- scenario: eez-xchain
  name: "EEZ Outbound (L2->L1)"
  config:
    mode: outbound
    throughput: 20            # independent dial — tune live in the UI
    max_pending: 200
    max_wallets: 40
    outbound_proxy: "0x..." # from create_l2_proxy (wave-test.sh)
```

An inbound-only spammer needs none of the `outbound_*` fields, and vice
versa — each side only requires the config for the direction it drives.

## Scenario config reference (`eez-xchain`)

| Key | Meaning |
|---|---|
| `attack` | `""` (well-formed setValue), `garbage-calldata`, or `revert` — adversarial mode for DDoS-resilience testing. Run as a separate spammer (see below). |
| `mode` | Source chain only: `inbound` on `spamoor-eez-inbound`, or `outbound` on `spamoor-eez-outbound`. Transaction types are selected with `ops`. |
| `ops` | Op kinds to cycle through per direction: any of `set`, `noret`, `value`, `wrapper`. Empty = `[set]` (setter-only). Only the proxies/wrapper for the listed ops are required. Ignored when `attack` is set. |
| `inbound_weight` / `outbound_weight` | Legacy single-direction aliases. At most one may be non-zero; prefer `mode`. |
| `throughput` | Cross-chain txs/slot for this direction. Runs forever unless `total_count` is set. |
| `total_count` | Hard cap for this direction; `0` = unlimited. |
| `inbound_front` / `outbound_front` | Front endpoints, defaulted to eez-node's `:18999` / `:18998`. The matching daemon funds wallets over its normal chain RPC; only scenario transactions go to the front. |
| `inbound_proxy` / `outbound_proxy` | Pre-created **setter** proxy per direction (op `set`). Required when that direction has weight and `set` is in `ops`. |
| `inbound_noret_proxy` / `outbound_noret_proxy` | Pre-created **ValueNoRet** proxy (op `noret`). |
| `inbound_deposit_proxy` / `outbound_withdraw_proxy` | Pre-created recipient proxy for **value transfers** (op `value`): deposit inbound, withdraw outbound. |
| `inbound_wrapper` / `outbound_wrapper` | Pre-created **wrapper** contract over the setter proxy (op `wrapper`). |
| `value_max` | Upper bound for the random `setValue()` argument (well-formed load only; `0` sends a fixed `1`). |
| `base_fee` / `tip_fee` / `base_fee_wei` / `tip_fee_wei` | Same fee knobs as native scenarios — use the `_wei` variants for L2's sub-gwei fees if needed. |

## Blockspace calibration (uncalibrated — first-run TODO)

The existing L1 baseline table (README.md "spamoor — continuous L1 load")
was measured empirically: `eoatx` is a fixed 21,000 gas, so
`throughput × 21,000 / 60,000,000` is exact. Cross-chain `setValue` calls
are **not** fixed-gas — `gas_limit: 600000` is a ceiling passed to the tx
builder, not the actual execution cost, which depends on cross-chain
relay/proxy internals. So:

- The `throughput: 20` placeholders in the directional startup examples are a
  starting guess, not a calibrated value.
- Inbound load lands on **L2** blockspace (60M gas / **2s** slot — 6x faster
  than L1's 12s, so the same tx/slot number is a much smaller % of L2 than
  the equivalent number would be of L1).
- Outbound load lands on **L1** blockspace (60M gas / 12s slot), on top of
  whatever the L1 baseline spammer and batch-posting are already using.

**Before relying on a specific % target**, run the enclave with `eez-xchain`
at a few throughput levels, watch actual gas usage per block on each chain
(`cast block <n> --rpc-url <L1|L2> --json | jq .gasUsed`), and write the
measured throughput→utilization numbers here. Until then, treat `throughput`
as a relative dial you tune while watching the devnet, not a precise %
target — same caveat applies to the L1 baseline number if you're running it
concurrently with heavy cross-chain load, since batch-posting is extra L1
gas the original table didn't account for.

## Adversarial / DDoS-resilience testing

Two layers, deliberately separate so healthy and malicious load are distinct
spammers you dial independently (crank the attack, watch whether legit
throughput degrades — that's the test):

### Generic (native spamoor scenarios, no plugin code)

For mempool/blockspace floods that aren't cross-chain-specific, use spamoor's
built-in scenarios as additional spammers (via the daemon UI or a
startup-spammers entry) pointed at the normal L1/L2 RPC or the fronts:

- `tx-fuzz-invalid` — malformed / invalid transactions
- `gasburnertx` — max-gas block-filling (blockspace starvation)
- `storagespam` — state bloat

### Cross-chain-specific (the `attack` option)

Set `attack` on an `eez-xchain` spammer to fire malformed traffic at the
pre-created proxies via the fronts — all other config (mode/weights, rate,
proxies) is unchanged. A rejected or reverting tx is the *expected* outcome,
not a failure: the scenario logs it at debug and keeps firing, so you're
measuring the node's resilience, not chasing green receipts.

| `attack` | Payload | Probes |
|---|---|---|
| `garbage-calldata` | Random 4–68 bytes to the proxy | Front admission gate + decode path; target fallback handling |
| `revert` | Valid 4-byte selector for a function the target lacks + junk args | The cross-chain revert/rollback settlement path at volume |

Because attack is just an option, an attack spammer is a normal `eez-xchain`
config entry with `attack` set — run it as a **separate spammer** next to a
healthy one (`attack` unset) and watch whether the healthy side's inclusion
rate holds as you crank the attack throughput:

```yaml
- scenario: eez-xchain
  name: "EEZ Attack (garbage inbound)"
  config:
    attack: garbage-calldata
    mode: inbound
    throughput: 40
    max_pending: 400
    max_wallets: 40
    inbound_proxy: "0x..."   # a pre-created L1 proxy to hammer
```

### Not yet built: proxy-creation spam

The highest-value cross-chain DoS vector — `createCrossChainProxy` is
permissionless (any funded key; see `devnet-test.sh`), so an attacker can mint
unbounded proxies to bloat registry/state. It's **not** an `attack` mode yet
because it differs structurally: proxy creation is a plain contract call that
goes to the **normal L1/L2 RPC**, not the cross-chain fronts (see how
`wave-test.sh`'s `create_l1_proxy`/`create_l2_proxy` broadcast), and it needs
the registry/CCM addresses + rollup ids rather than a proxy address. Worth
adding as a third `attack` mode once the base scenario is verified against a
live enclave (so the front-vs-RPC routing can be confirmed rather than
guessed).
