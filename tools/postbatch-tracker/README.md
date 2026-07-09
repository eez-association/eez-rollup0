# PostBatch Tracker

A tiny zero-build web UI to track all `BatchPosted` events on the EEZ L1
registry for a deployment — who posted each batch, when, whether it reverted,
and what it changed (state updates, entries consumed, L2 executions) in the
same transaction.

## Run

```bash
cd tools/postbatch-tracker
./serve.py                # no params — reads .env + deployments.env
./serve.py --port 9000    # only the listen port is optional
```

Open <http://localhost:8080/>. No parameters: `serve.py` walks up to the repo
root and reads the L1 RPC, registry address and deploy block straight from
`.env` and `deployments.env`:

- `EEZ_TOOL_L1_RPC_URL`   → optional browser-tool L1 override
  (default `http://127.0.0.1:18645`; proxied at `/rpc`)
- `EEZ_REGISTRY_ADDRESS`  → registry to scan
- `EEZ_REGISTRY_DEPLOY_BLOCK` → scan start block
- `EEZ_L1_EXPLORER_URL`   → block-explorer base for tx/address links
  (default `https://gnosis-chiado.blockscout.com`)
- `EEZ_TOOL_L2_RPC_URL`   → optional browser-tool L2 override, used to resolve
  real L2 block numbers
  (default `http://127.0.0.1:18688`; proxied at `/l2rpc`)

The page fetches these from `GET /config` on load, so it always reflects the
current deployment. Fields stay editable in the UI; manual edits are remembered
in `localStorage` only while the registry matches, so switching deployments
isn't masked by stale values. Click **Load** for a full (re)scan from the start
block; **Auto** polls **every 5s incrementally** — it only scans L1 blocks newer
than the last scan and prepends the new batches on top (de-duped at the rescan
boundary), accumulating the stats instead of re-fetching everything.

`.env`/`deployments.env` values can be overridden by real environment variables
of the same name.

## Why the proxy?

The L1 node doesn't send CORS headers, so a browser can't call it
cross-origin. `serve.py` serves the page and proxies `POST /rpc` to
`EEZ_L1_RPC_URL`, adding CORS. The page's RPC field defaults to `/rpc` for this
reason — point it at a full URL only if your node already sends CORS.

## What it reads

- Event: `BatchPosted(uint256 indexed rollupCount)` on the registry
  (`EEZ_REGISTRY_ADDRESS`), scanned from `EEZ_REGISTRY_DEPLOY_BLOCK` to head
  in block chunks.
- Per batch it fetches the tx + receipt to show the submitter, success/revert,
  and decodes the other EEZ events in the same tx: `StateUpdated`,
  `ExecutionConsumed`, `L2ExecutionPerformed`, `L2TXExecuted`,
  `ImmediateEntrySkipped`.
- Tx hash and submitter are links to the block explorer.
- **L2 blocks** column: every batch shows the exact *count* of L2 blocks it
  commits (`block_tx_counts.length`). The absolute L2 block *range* cannot be
  derived from L1 alone — naive accumulation from genesis breaks under batch
  re-posts / L2 resets — so it is resolved against the L2 node (`/l2rpc`):
  each batch is anchored by looking up its first user tx (`eth_getTransactionByHash`
  → L2 block), and the range is `[block − firstTxOffset, +blockCount−1]`. This is
  reset-proof. Batches with no user tx are then filled by **verified contiguity**:
  a run of un-anchored batches sitting between two anchored ones gets contiguous
  ranges (`from = prev.end+1`) **only when** the run's block counts exactly bridge
  the two anchors' L2 blocks (otherwise a re-post sits in between → left
  unresolved). These show as `≈start–end` (muted). Anything that can't be anchored
  or verified shows only the count.
- **Batch payload → decode**: each row has a `decode` toggle that parses the
  `postAndVerifyBatch(ProofSystemBatchPerVerificationEntries)` calldata using
  the contract ABI (served at `/abi` from the Foundry artifact at
  `contracts/out/EEZ.sol/EEZ.json`, falling back to the source branch paths —
  run `forge build` if missing).
  The panel shows, as nested tables:
  - a chip summary (entries, transient count, proof systems, rollup ids, block
    binding, blob indices, calldata size);
  - the decoded **`callData` DA payload** — `tag`, per-block tx counts, the
    user **transactions** (each EIP-2718 tx parsed: type/from/to/value/nonce/
    gas/hash) and the **l2Entries** (ABI-decoded `ExecutionEntry`s);
  - the full decoded `postAndVerifyBatch` batch struct.

  The `callData` format is `0x00 ‖ RLP([blockTxCounts, transactions,
  l2Entries])` (see `crates/eez-payload-codec`); txs are RLP `Vec<u8>` (byte
  lists) and l2Entries are ABI-encoded with the same `ExecutionEntry` type as
  the on-chain `entries`.

Single file (`index.html`) + `serve.py`, no dependencies beyond Python 3 and a
browser. `ethers` loads from a CDN.
