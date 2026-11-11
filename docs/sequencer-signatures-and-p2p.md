# Sequencer signatures and P2P in eez-node

How eez-node implements [`sequencer-p2p-spec.md`](sequencer-p2p-spec.md): its
configuration, how the parts are wired, and the design decisions behind it.
The protocol itself (messages, validation, gossip, discovery, fork choice and
joining) is defined only in the spec.

## Why the signature is not in the header

L1 commits L2 block hashes, so any header bytes become part of the
commitment. Every Sync block claims 1+N candidate hashes
(`eez-composer/src/local/build.rs`), any of which can settle. A signature in
the header would have to be produced for every candidate and shipped in the
DA. No header field can hold it anyway:

- `requests_hash` must equal the empty-requests hash. Our executor emits no
  requests, and reth checks this after execution.
- `extra_data` is capped at 32 bytes in three places: alloy's payload
  conversion, reth consensus and our codec.
- `mix_hash`, `nonce` and `parent_beacon_block_root` are execution inputs or
  forced to zero.

So the signature travels next to the block and signs its hash.

## Configuration

| Variable | Binary | Effect |
|---|---|---|
| `EEZ_SEQUENCER_KEY` | eez-node (composer) | signs heads; unset: the composer logs a WARN and doesn't sign |
| `EEZ_SEQUENCER_ADDRESS` | eez-follower | the authorized signer; turns on the head follower |
| `EEZ_P2P_LISTEN` | both | overlay TCP multiaddr with a fixed port; joins the overlay |
| `EEZ_P2P_PEERS` | both | static overlay peers, comma-separated, each ending in `/p2p/<id>`, IP only |
| `EEZ_SYNC_L1_ONLY` | both | `1` or `true`: derive history from L1 instead of peers |

Startup rules:
- A composer with `EEZ_P2P_LISTEN` and no `EEZ_SEQUENCER_KEY` refuses to
  start.
- On a follower, `EEZ_SEQUENCER_ADDRESS` and `EEZ_P2P_LISTEN` each require the
  other. With neither, the follower follows L1 only.
- The stateful prover (`EEZ_STATEFUL_PROOF_SIGNER_ADDR`) moves the unsafe head
  itself, so it requires a follower without `EEZ_SEQUENCER_ADDRESS`.
- `EEZ_SYNC_L1_ONLY` accepts `1`, `true`, `0`, `false` or empty; anything else
  is a startup error.

`node_cli` (`eez-node-common/src/lib.rs`) adds flags to every L2 node:
- `--disable-tx-gossip`: a transaction enters only through the receiving
  node's RPC, and cross-chain transactions only through the composer's fronts.
- `--debug.startup-sync-state-idle`: makes reth's sync state exact, so boot can
  wait for an idle engine.
- It refuses to start with discovery on and no `--bootnodes`. Otherwise reth
  would dial Ethereum mainnet's bootnodes for a custom chain.

## Composer

- **One funnel.** The BlockCommitter actor is the only writer of the canonical
  head. Every head change goes through `set_head`
  (`eez-driver/src/block_committer.rs`), which publishes the header on a
  `tokio::sync::watch`. The actor never blocks on readers, and any new
  head-changing path is signed automatically.
- **The signer** (`eez-node/src/head_signer.rs`):
  - reads the watch;
  - loads the block, waiting up to 1 s for it to become readable;
  - signs the head with the next counter and stores the envelope in a second
    watch.

  A head replaced before its block is readable is skipped, and so is every
  intermediate head the signer never read. Some heads also stay unsigned
  until the head changes again: a head whose block stays unreadable for 1 s,
  or whose read or signing fails, is logged and not retried. That falls short
  of the spec's "sign every change".
- **The overlay** publishes from that store. The signer starts only after boot
  ([Boot](#boot)), so a syncing composer never signs stale heads under its new
  epoch.

## Follower

- **The overlay** validates each message and reports the verdict to gossipsub
  before forwarding. Accepted heads reach the follower through a bounded
  channel with `try_send`. When the channel is full, the head is dropped with a
  WARN and not retried. The overlay has already recorded the head, so it
  ignores later copies. The follower catches up on the next newer head.
  Rejects are summarised in one WARN per 5 s dial tick
  (`eez.p2p.message.rejects`).
- **The head follower** (`eez-follower/src/unsafe_head.rs`) runs the spec's
  fork choice:
  - It retries the target every L2 block time and on every accepted head.
  - It fetches missing blocks from devp2p peers through reth's `FetchClient`:
    - headers backward from the hash, then bodies, up to 64 per request;
    - each request is time-boxed to 2 s, because reth queues requests forever
      without peers.
  - It imports under `begin_reconcile`, so imports never interleave with a
    Deriver batch:
    - new blocks through `commit_derived`;
    - a known head through `reorg_to`.
  - It holds while the Deriver's batch index is ahead of safe, so it never
    rewrites heights L1 has settled. The hold is checked both before fetching
    and under the lock.
- **Why descent from safe is checked here:** reth only requires safe to be
  known, so it would canonicalize a head whose ancestry conflicts with safe.

## Boot

Peers mode follows the spec's joining procedure. Code is in
`eez-node-common/src/boot.rs`.

- **Wait for an idle engine.** A node killed mid-backfill restarts with reth
  resuming it, and reth answers SYNCING to every FCU until it finishes. Boot
  sends nothing until reth is idle, and spawns the committer only after:
  a head read earlier is stale.
- **Peers mode (default):**
  1. Read `F` and `H_F`.
  2. If `H_F` is local, seed there.
  3. Otherwise:
     - rewind to the node's own finalized block `f`;
     - repeat `sync_to(H_F)` (FCU head = safe = finalized = `H_F`) every 2 s,
       re-reading `F` each time;
     - after VALID, `adopt_synced` points the committer's mirrors at `H_F`;
     - check that `f`'s parent is still canonical.

     reth backfills with its pipeline when the gap is over 32 blocks, and
     downloads the range otherwise.
  4. Seed the Deriver with one synthetic index record at `F`. Every post at or
     before `F` reads as processed. The record is a floor: an L1 reorg below
     it is fatal, never an L2 rewind.
  5. Reconcile `F` to L1 head from L1.

  Peers mode neither reads nor writes the boot checkpoint.
- **L1-only mode:** derives from the boot checkpoint, or else from the deploy
  block.
  - It fails with "L1 history incomplete" when L1 no longer serves a block it
    needs.
  - Once its scan passes `F`, the local chain must hold `H_F`. If the first
    read of `F` fails transiently, boot logs
    `eez.node.sync.history_check_skipped` and skips this check.
  - A restart from the checkpoint keeps reth's finalized block, capped at the
    checkpoint's L2 block.
- **Composers then drop their unsettled tail** (`drop_unsettled_tail`):
  - The composer rewinds to L1 safe before the sequencer starts.
  - Transactions in the dropped blocks are lost and logged by hash. The held
    pool doesn't survive a restart, and reth doesn't re-add rewound
    transactions.
  - A postBatch in flight from before the restart may still land. The Deriver
    then replays L1's version.
- **Then the gate opens**, in this order:
  - the Deriver loop, then the stateful prover;
  - followers: the overlay, then the head follower;
  - composers: the overlay, the head signer, then the sequencer;
  - the L1 watcher, from `F`.

## Deployment

- Every node that may need history has devp2p peers: `--trusted-peers` with
  the composer's enode, or `--bootnodes` with discovery on.
- History nodes keep full block history, so not `--minimal`. A network needs
  at least two independent ones; on a small network the composer is often
  the only one.
- The genesis composer, and the stateful prover (its only peer is the
  composer), run with `EEZ_SYNC_L1_ONLY=1`.

## Known limits

- **A resume of `H_F`'s block that lands after `F` stops a booting node**
  (`FinalizedResume` in peers mode, "L1 history incomplete" in L1-only).
  Finalizing `H_F`'s parent instead would close it; that waits on what
  `finalized` must mean for bridges.
- **Blob DA**, when it comes, needs a beacon endpoint for the window.
- **Old history's DA isn't re-checked:** v0 trusts L1 settlements.
- **The receiver accepts several signature encodings** (spec §3.3). A strict
  rule is coming.

## Decisions

**Owner:**
- v0 trusts L1 settlements unconditionally.
- Signatures exist only on the overlay, never in the DA.
- Ordering is by an ephemeral version, not a persisted counter.
- Several composers may sign with one shared key. A follower follows one of
  them, and L1 still decides settled blocks.
- Overlay discovery runs through reth's discv5, and discovery is on.
- Heads come only from gossip, and blocks only from peers; there is no
  sequencer RPC.
- History comes from peers by default. L1-only derivation runs only with
  `EEZ_SYNC_L1_ONLY`, and a node without peers waits.
- Long-range sync uses reth's own sync, triggered by an FCU. Every boot takes
  one path, anchored at L1-finalized.
- A restarted composer drops its unsettled tail. Persisting the held pool is a
  separate effort.
- Sequencer promote and demote are manual, in a later change.
- Other clients run their own followers on the overlay and sync over devp2p.
  They don't sit behind an eez-node.

**Implementation:**
- EIP-712 for the message, so a KMS signer, Solidity or a multisig can work
  with it.
- The 10 s future bound.
- `watch` channels for heads and signed heads, so the committer never blocks.
- Fork-choice candidates must descend from safe, so failover between
  composers follows L1 facts, not timers.
- Anonymous gossipsub, with the sequencer signature as the only
  authentication; Reject without peer penalties until peer scoring lands.
- The reth node key is the overlay identity.
- The unsafe tail uses the follower's own fetcher, not reth's sync. reth
  checks safe only after it canonicalizes a synced head.
- "Wrong chain" means the parent of the node's own finalized block is gone
  after the sync. A replaced block at `H_F`'s height is a normal restart.
