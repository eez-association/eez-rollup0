# L1 batch scan

The scan reports, from L1 alone, what L1 applied of every `postAndVerifyBatch`
call listing this rollup. `:N` cites `eez-core-protocol/src/EEZ.sol` at `143d87e`.

## Model

The scan is a pure function of one L1 block: queues live for one block
(`:1578-1582`), so no fact crosses a block boundary.

```text
BatchPosted logs -> posts       where each call ended, which queues it reset
call bytes       -> calls       direct: tx.input; contract: a committed trace frame
EEZ logs         -> records     one per applied entry; a skip per skipped immediate
calls + queues   -> ownership   in a call: that post; else the queue's latest lister
owner's bytes    -> settlement  exact entry index; rolling hash and roots checked
```

Only transactions holding a post that lists this rollup are fetched; a post
not listing it is never decoded but still resets its queues.

## Records

```text
item   := Posted | Skipped(entryIndex) | Record
Record := Consumed(callHash, queue, index)?  RootSet(rollup, root)+  Executed(index, rollingHash)
```

`Posted` is `BatchPosted` (last in its call, `:470-475`, listing the queues it
reset, `:804-811`), `Skipped` is `L2TxSkipped`, `Consumed` is
`ExecutionConsumed`, `RootSet` is `L2ExecutionPerformed` (one per
`rollupUpdate`) and `Executed` is `EntryExecuted`. Posts never nest (`:380`)
nor do entries (`:871-881`), so any other shape is `Decode`. `Executed.index`
equals `Consumed.index` if present (`:951`), else `0` (`:495-496`); root rollup
ids strictly increase (`:578-584`).

## Calls

- **Direct post.** `tx.to` is EEZ, or the configured settlement wrapper, and the
  input starts with the selector. The wrapper forwards that calldata in one
  EEZ call, so the bytes are `tx.input`. `sender` is `tx.from` for a direct EEZ
  post and the wrapper for a wrapper post. `tx_sender` is the outer signer
  either way. Every scanned log is the post's.
- **Contract post.** Anything else (router, Safe, ERC-4337, EIP-7702,
  `EEZPostBatcher`) may build the bytes in memory, so the scan traces the block
  with `debug_traceBlockByHash` (`callTracer`, `withLog`), pinned by hash.
  `tx_sender` is the traced transaction's signer.

The walk drops every frame with an `error` or `revertReason`, and its subtree.
A call is a committed `CALL` to EEZ with the selector, sent by the frame's
`from` (never a `DELEGATECALL` or `CALLCODE`). The k-th call pairs with the
k-th `BatchPosted` of its transaction; its scanned logs are contiguous and end
there. Fewer calls than posts is `Unsupported`; more calls, a log count off
from `eth_getLogs`, a misplaced or overlapping range, or a straddling record
is `Decode`.

## Verification

Each call whose post lists this rollup is checked against its own `BatchPosted`:

1. the bytes decode as `postAndVerifyBatch`, else `Decode`;
2. `rollupIdsWithProofSystems[].rollupId` equals `rollupIds`, else `Decode`;
3. no `blobIndices` and `blockNumber == 0`; or, in a blob-carrying tx under
   the configured `Rollup0Deployment`, a post listing only this rollup must be
   a candidate-V1 carrier: `blockNumber == uint64.max`, empty `callData` and
   `blobIndices == [0, n)` over the tx's `n` blobs (`Decode` otherwise). Any
   other shape is `Unsupported`;
4. the shared public input equals `sharedPublicInput`, else `Decode`, or
   `Unsupported` when another listed rollup may bind `customData` the scan
   cannot reproduce. A Rollup0 blob post binds the tx's versioned hashes and
   manager data derived from its own block's timestamp and parent. Historical
   posts use their committed EEZ caller as the wrapper, so rotating the wrapper
   does not invalidate earlier history. The proof signer authenticates the
   active wrapper from its own L1 read before signing a new candidate.

Derivation fetches a settled, unprocessed candidate-V1 post's blobs from
`EEZ_L1_BEACON_URL` and keeps only those matching its versioned hashes.

The hash does not bind `immediateEntryCount`, `expectedRootPerRollup` or the
proofs; the scan reads them from the exact bytes EEZ received.

## Ownership and index mapping

In a transaction holding a post that lists this rollup, an item inside a call
is that call's post's. A record updating this rollup outside every call must
carry `Consumed` and is owned by the latest earlier post listing
`Consumed.queue`, which must list this rollup: ownership follows the queue, not
the rollup in `L2ExecutionPerformed`. `Decode`: a queue with no lister, an
owner not listing this rollup, a record for it in a call whose post does not
list it, and a skip or `Consumed`-less record between such a transaction's
calls, since only a post's leading run emits them (`:414-436`).

`iec` is `immediateEntryCount` and `i` the leading run: the first `k < iec`
with a non-zero `proxyEntryHash`, else `iec` (`:414-431`).

| Item | Entry index | Order check |
|---|---|---|
| In-call `Skipped(k)` | `k` | next immediate index, `k < i` |
| In-call record, no `Consumed` | next immediate index | below `i` |
| In-call record with `Consumed` | `i + Consumed.index` | below `iec`, rising; no leading-run item after it |
| Per-queue record | `Consumed.index`-th of `entries[iec..]` with `destinationRollupId == queue` (`:816-821`) | rising per queue |

At its `BatchPosted` a post has one item per immediate below `i`. Each record
must match its entry, else `Decode`: `callHash` and `queue` (`:1304-1320`),
`rollingHash` (`:1026`), and the `RootSet` sequence against `rollupUpdates`
(`:1327-1345`). Consumption may skip entries (`:1273-1289`), so gaps are normal.

## Errors

| Variant | Class | Raised for |
|---|---|---|
| `Provider` | retryable | transport; a malformed `get_logs` answer; a trace failure other than `-32601`, including a post's transaction failing or missing |
| `SourceIncomplete` | retryable | two hashes for one block; a transaction or block the endpoint does not serve yet; unavailable blob data |
| `Decode` | terminal | undecodable bytes; a `rollupIds` or commitment mismatch; a grammar, pairing, ownership or entry-match violation |
| `Unsupported` | terminal | the shapes below; a multi-rollup commitment mismatch; a post not made by a selector `CALL` |
| `TraceUnavailable` | terminal | `-32601` (no `debug` namespace) on a block that needs a trace |

The watcher and the settlement observer retry a retryable error on their next
poll. Boot reconcile retries with backoff, but gives up after
`BOOT_CATCH_UP_MAX_TRANSPORT_FAILURES` `Provider` errors. A terminal one panics
the watcher (invariant 7), fails boot reconcile and fails the observed bundle.

## Operation

Only a block holding a contract post that lists this rollup is traced; the
composer posts directly. `EEZ_L1_TRACE_RPC_URL` (default `EEZ_L1_RPC_URL`) must
serve `debug` and hold the state at the parent of each traced block, so an old
contract post needs an archive node. The embedded and kurtosis L1s serve
`debug` over HTTP; keep it private. Any other failure to trace the block, or a
post's transaction in it, retries and logs `eez.l1_scan.trace_failed` at ERROR;
other transactions' trace errors are ignored. A failing trace holds the watcher
at that block, and with it `NewHead`, the safe head and the composer's slots.
The signer binds no sender, so anyone can re-post a batch through a contract
and force every node to trace that block, stalling it on a trace over the
endpoint's limits.

### Trace latency (measured)

`debug_traceBlockByHash` (callTracer, withLog) against the node's embedded L1
(reth 2.7.0) on the kurtosis 12 s L1, sequential, 2026-10-18:

| sample | txs/block | p50 | p95 | max | max size |
|---|---:|---:|---:|---:|---:|
| last 50 blocks under cross-chain load | 2.4 | 1.1 ms | 2.6 ms | 4.3 ms | 36.3 KiB |
| 8 blocks holding a contract post | 3–7 | 1.6 ms | 1.8 ms | 1.8 ms | 38.7 KiB |

A trace costs milliseconds against a 12 s slot.

## Unsupported shapes

- **Other blob profiles.** The scan accepts only the configured Rollup0 blob
  carrier. It cannot reproduce another rollup's manager data.
- **Other nonzero block numbers.** `customData` may read arbitrary manager
  state, so the call alone cannot reproduce it.
- **`upgradeToAndCall` posts.** They run by `DELEGATECALL` into a proxied EEZ;
  ours is not proxied.
