# eez-rollup spamoor plugin

A [spamoor](https://github.com/ethpandaops/spamoor) plugin (Yaegi-interpreted,
no build step) providing the `eez-xchain` scenario: continuous inbound
(L1→L2) and/or outbound (L2→L1) cross-chain `setValue` load against the EEZ
devnet, run alongside the existing L1-only `eoatx` baseline.

Runs inside the enclave as the `spamoor-eez` service (`infra/kurtosis/main.star`),
in `spamoor-daemon` mode so throughput is adjustable live via its web UI
(`kurtosis port print eez-devnet spamoor-eez http`) or REST API, without
restarting the enclave — see the daemon's `docs/plugin-system.md` (upstream)
for the API. Leave the enclave running for as long as you want continuous
load, the same way `reorg-scheduler.sh` is meant to be left running
(README.md's existing "always-on box" note applies here too).

## Prerequisites: this scenario does not provision proxies

`eez-xchain` only drives load against **already-created** cross-chain
proxies — it doesn't deploy targets or create proxies itself. Create them the
same way `infra/kurtosis/scripts/wave-test.sh` does:

- Inbound proxy: `create_l1_proxy <L2 Value target>` — a `CrossChainProxy` on
  the shared L1, created via `createCrossChainProxy(registry, target, rollupId)`.
- Outbound proxy: `create_l2_proxy <L1 Value target>` — created via
  `computeCrossChainProxyAddress` + `createCrossChainProxy` on the L2 CCM
  predeploy (`EEZ_CCM_L2_ADDRESS`).

Easiest path: run `wave-test.sh` once (any mode) against the enclave, then
reuse the proxy addresses it printed (`inbound proxies: setter=...`,
`outbound proxies: setter=...`).

## Adding the cross-chain spammer

The daemon boots with **no** startup spammers by default — the cross-chain
scenario needs proxy addresses that don't exist until after deploy. Once you
have real proxy addresses and a funded L2 key, add the cross-chain load either
via the daemon web UI (`kurtosis port print eez-devnet spamoor-eez http`, add
a spammer using the `eez-xchain` scenario — it appears once the plugin loads,
confirm on the `/plugins` page) or by baking it into a fresh enclave (copy
`startup-spammers.example.yaml` to a gitignored `startup-spammers.yaml`, fill
in the addresses, point `eez.spamoor_eez.startup_spammer_config` at it, and
re-run `up.sh`).

### Recommended: two independent spammers (inbound + outbound)

For real testing, run inbound and outbound as **two separate spammers** rather
than one `mode: mixed` entry. This gives each direction its own live
throughput dial (throttle one without touching the other) and isolates
failures (an outbound-front stall can't starve the inbound side's pending-tx
budget). "Mixed" load = both enabled; "inbound-only" / "outbound-only" =
disable the other. This also matches spamoor's own idiom (its built-in
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
    outbound_rpc: "http://eez-node:18998"   # l2-xchain front
    outbound_private_key: "0x..."           # funded L2 key
    outbound_proxy: "0x..."                 # from create_l2_proxy (wave-test.sh)
```

An inbound-only spammer needs none of the `outbound_*` fields, and vice
versa — each side only requires the config for the direction it drives.

### Alternative: single `mode: mixed` entry

When you just want balanced cross-chain load from one knob, a single entry
with `mode: mixed` (or an explicit `inbound_weight`/`outbound_weight` ratio)
drives both directions interleaved in one process. Simpler, but the two
directions share one `throughput` dial and one `max_pending` budget — you
can't retune the ratio without restarting the spammer, and a stall on one
side can throttle the other. It must supply both sides' config
(`inbound_proxy` **and** `outbound_*`).

## Scenario config reference (`eez-xchain`)

| Key | Meaning |
|---|---|
| `attack` | `""` (well-formed setValue), `garbage-calldata`, or `revert` — adversarial mode for DDoS-resilience testing. Run as a separate spammer (see below). |
| `mode` | `inbound`, `outbound`, or `mixed` (1:1) — shorthand for the weight pair below. |
| `inbound_weight` / `outbound_weight` | Explicit mix ratio; overrides `mode` if either is non-zero. In a single-direction spammer only one is non-zero, so only that side's config is required. |
| `throughput` | Cross-chain txs/slot (rate). Runs forever unless `total_count` is set. Split across directions by weight. |
| `total_count` | Hard cap: send exactly this many txs (split by weight), then stop. `0` = unlimited. Set either/both of `throughput`/`total_count`. |
| `outbound_rpc` / `outbound_private_key` | Required iff `outbound_weight > 0` — the L2 rollup has a distinct chain id from L1 (see `infra/kurtosis/genesis.json` vs the L1 `network_id`), so spamoor's single-chain-id client pool can't hold both; the plugin builds a second pool itself from these. |
| `inbound_proxy` / `outbound_proxy` | Pre-created proxy addresses (see above). Required per the corresponding non-zero weight. |
| `value_max` | Upper bound for the random `setValue()` argument (well-formed load only; `0` sends a fixed `1`). |
| `base_fee` / `tip_fee` / `base_fee_wei` / `tip_fee_wei` | Same fee knobs as native scenarios — use the `_wei` variants for L2's sub-gwei fees if needed. |

## Blockspace calibration (uncalibrated — first-run TODO)

The existing L1 baseline table (README.md "spamoor — continuous L1 load")
was measured empirically: `eoatx` is a fixed 21,000 gas, so
`throughput × 21,000 / 60,000,000` is exact. Cross-chain `setValue` calls
are **not** fixed-gas — `gas_limit: 600000` is a ceiling passed to the tx
builder, not the actual execution cost, which depends on cross-chain
relay/proxy internals. So:

- The `throughput: 20` placeholder in `startup-spammers.example.yaml` is a
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
