# Sequencer signatures and P2P

The sequencer signs every change of its canonical L2 head. Peers use the
signature to decide which unsafe head to follow, and they carry signed heads
to each other over a P2P overlay. This is proof of authority on block
production. It is not a preconfirmation, and it does not touch settlement:
L1 alone decides safe and finalized.

`:N` cites the tree at `cf31950e` (main after #212).

## Versions

| Version | PR | Scope |
|---|---|---|
| seq-sig-v0 | 1 | Signed heads, follower fork choice, delivery over the sequencer RPC |
| p2p-v0 | 2, stacked on 1 | The same signed heads over libp2p gossip; missing blocks by hash over devp2p |
| sync-v0 | 3, stacked on 2 | History from peers through reth's sync, anchored at L1-finalized; L1-only derivation as a flag; restarted composers drop their unsettled tail |

Later versions:
- read the signer set from L1;
- have the prover check the signature;
- gate settlement on the sequencer;
- support multisig;
- add the signature to the DA if that proves necessary.

## Goals

- A follower's unsafe head tracks the sequencer's head, including every block
  the sequencer replaces.
- A follower never adopts a head that its L1-derived safe block contradicts.
- Delivery is independent of transport. The signed message and the fork choice
  are identical whether a head arrives over RPC or gossip.

## Non-goals (v0)

- Settlement. L1 settlements are trusted as they are, so whatever the prover
  attests is valid. The signature never reaches L1, the DA or the prover.
- Signer sets read from L1, and multisig.
- Blob archives, and persisting the held pool.

## Anti-goals

- Changing the block. Header, block hash, L1 commitment, payload codec and
  prover inputs stay byte-identical.
- Composer liveness depending on the network. Signing and publishing never
  block the BlockCommitter and never panic on back-pressure.
- Silent drops. Every rejected head is logged with its reason (invariant 7).

## Why the signature is not in the header

L1 commits L2 block hashes, so any header bytes become part of the
commitment. Every Sync block claims 1+N candidate hashes
(`eez-composer/src/local/build.rs:560`), any of which can settle.

A signature in the header would therefore have to be produced for every
candidate and shipped in the DA. No header field can hold it anyway:

- `requests_hash` must equal the empty-requests hash. Our executor emits no
  requests (`eez-evm/src/block.rs:113`), and reth checks this after execution.
- `extra_data` is capped at 32 bytes in three places: alloy's payload
  conversion (which can't be configured), reth consensus, and our codec
  (`eez-payload-codec/src/lib.rs:52`).
- `mix_hash`, `nonce` and `parent_beacon_block_root` are execution inputs or
  forced to zero.

The signature therefore travels next to the block and signs its hash. The
hash commits to every header field and transaction, and through parent hashes
to every ancestor.

## Heads

```text
finalized  <- L2 hash stored by L1, as of an L1-finalized block   (Deriver)
safe       <- L2 hash stored by L1, as of L1 latest               (Deriver)
unsafe     <- newest sequencer-signed head descending from safe   (head follower)
```

Each source moves only its own head, and L1 always wins. The Deriver keeps
sole ownership of safe and finalized through `advance_safe_finalized`.

## Signed head

The signed message is EIP-712 typed data:

```text
domain   EIP712Domain(name "EEZ Sequencer", version "0",
                      chainId = L2 chain id, verifyingContract = EEZ registry)
struct   SignedHead(uint64 rollupId, uint64 number, bytes32 blockHash,
                    bytes32 parentHash, uint64 epochMs, uint64 counter)
```

The registry address in the domain separates two deployments that reuse an L2
chain id. EIP-712 lets the key live in any `alloy_signer::Signer`, KMS
included. Solidity can verify the signature later, and a multisig wallet can
sign the same struct.

Heads travel in an envelope:

```text
Envelope { head: SignedHead, signature: 65 bytes r‖s‖v, block: Option<consensus RLP> }
```

`block` carries the head block when it is new, and is absent for a rewind to
a block peers already have. The block uses the same consensus RLP the prover
stream carries.

A receiver checks two things before anything else:
1. `signature` recovers to the configured sequencer address.
2. If `block` is present, its `hash_slow()` equals `blockHash` and its parent
   equals `parentHash`.

### Version

`version = (epochMs, counter)`, compared in that order (lexicographically).

- **epochMs** is the sequencer process's wall-clock start time in
  milliseconds.
- **counter** starts at 1 and adds 1 for every signed head. It is held in
  memory and never persisted.

Each signing process is a **source**, identified by `(signer, epochMs)`.
Several composers may sign with the shared key, each under its own epoch.

- **Within a source,** the counter only moves forward. A head at or below its
  source's latest counter is a replay and is ignored.
- **Across sources,** the higher epoch wins. Followers therefore prefer the
  most recently started composer, and a restarted composer outranks its old
  self.
- The clock must move forward only between restarts, not between messages.

A follower handles unusual versions as follows:
- **Too far in the future:** an `epochMs` more than 10 s ahead of the
  follower's clock is rejected loudly. A composer with a wrong clock fails
  visibly instead of outranking every other source.
- **Same version, different `blockHash`:** equivocation, which is only
  possible through a bug or a stolen key. The follower keeps the first and
  logs ERROR.

## Sequencer: sign every head change

The BlockCommitter actor is the only writer of the canonical head. It writes
`unsafe_head_hash` and `last_header` directly in five places:

| Site | Cause |
|---|---|
| `process_reorg_to` `:583` | failed bundle → rewind; L1 reorg retreat |
| `forkchoice_or_demote_head` `:623` | `InvalidState` FCU → demote to safe |
| `process_advance_head` `:680` | follower bare FCU |
| `process_derive` `:720` | prebuilt Sync blocks; Deriver replays (partial settlement, suffix replay, resume-append) |
| `process_sequence` `:795` | Live and Future blocks |

All five are in `eez-driver/src/block_committer.rs`.

**One funnel.** A single `set_head(header, block)` replaces those five writes
and publishes a `HeadChange { header, block: Option<SealedBlock> }` on a
`tokio::sync::watch` channel. A `watch` keeps only the latest value, so the
actor never blocks and a slow reader can't lag it. Any new head-changing path
goes through the funnel, so its head is signed automatically.

**The signer.** In composer mode with `EEZ_SEQUENCER_KEY` set, a signer task:
1. reads the `watch`;
2. assigns the next version;
3. signs;
4. stores the latest envelope.

Intermediate heads the reader skipped are never signed. Followers fetch
missing ancestors by hash. `EEZ_SEQUENCER_KEY` is a dedicated key, distinct
from the proof, poster and system keys. If it is unset, the composer logs a
WARN at startup and doesn't sign.

How each replacement reaches followers:

| On the sequencer | Signed head | Follower |
|---|---|---|
| Sync block's bundle fails (`recover_failed_batch`, `composer.rs:1833`) | rewind to N-1, then empty N′, then descendants | moves back, then adopts N′ |
| Partial settlement: the Deriver rebuilds N\* | N\* (with block) | adopts N\*; its own Deriver later finds it already present |
| Suffix replay or resume-append | the new head (with block) | adopts it |
| L1 reorg (`retreat_l2_to_cursor`, `deriver.rs:1291`) | rewind to the cursor block | moves back, then follows re-sequenced heads |
| Committer demotion | rewind to safe | moves back |

## Follower: fork choice

The follower's state:
- `latest`: the newest validly signed, non-future envelope of each source. It
  is a bounded map; sources whose head falls below finalized are pruned.
- `target`: the highest-version envelope in `latest` that descends from the
  current safe block. It is recomputed whenever a head arrives or safe moves.
  With no candidate, the head stays where it is.

**Failover, decided by L1.** A composer that dies keeps the highest version
among the heads followers have seen. It stops being a candidate once L1
settles a block its chain doesn't contain, and the follower then moves to the
best live source. Failover therefore takes about one settlement, during which
safe keeps advancing.

The same rule makes a follower briefly switch to another composer's chain when
L1 settles that composer's block. It switches back once the preferred composer
reorgs onto the settled block. This is inherent in divergent unsafe chains.

The follower retries `target` on every tick until a newer head replaces it.
For `target`:

1. **Fetch.** Walk from `blockHash` back through parent hashes until a block
   is known locally, fetching each missing block by hash. Stop at
   `MAX_ANCESTRY_WALK` (`eez-follower/src/unsafe_head.rs:31`). Running out of
   walk leaves the head unchanged and is logged.
2. **Descend from safe.** The head must be at or above safe, with safe on its
   ancestry; otherwise skip it and log. reth does not enforce this: it only
   requires safe to be known (`unsafe_head.rs:8-13`).
   `check_extends_safe` (`:200`) is reused.
3. **Import.** Under `begin_reconcile` (`block_committer.rs:246`), so imports
   never interleave with a Deriver batch:
   - each missing block in order through `commit_derived` (`:442`, with
     `feed_witness=false`);
   - a head that is already known goes through `reorg_to` (`:388`).

If the Deriver later reorgs the head (a partial settlement, say), safe moves.
Signed heads that don't descend from the new safe are skipped until the
sequencer's heads do.

The rule is a pure state machine in the seq-sig crate. Given an envelope, local
facts and `latest`, it returns one of these decisions:
- Ignore(reason)
- Fetch(hashes)
- Adopt(blocks, head)
- Rewind(head)
- Equivocation

An async driver in the follower carries out each decision against the
BlockCommitter. Both PR 1 and PR 2 feed this one machine.

## seq-sig-v0 (PR 1)

**Crate `eez-seq-sig`** (my name): `SignedHead`, `Envelope`, version
ordering, signing and verification over `alloy_sol_types` EIP-712, and the
fork-choice machine.

**Composer:**
- the funnel and the signer above;
- `eez_getSignedHead()`, which returns the latest envelope;
- `eez_getRawBlock(hash)`, which returns canonical consensus RLP or null.

The ancestors a follower needs are canonical on the sequencer, so the provider
serves them and no cache is needed. A dedicated method avoids requiring the
`debug` namespace, which deployments don't expose (`--http.api=eth,net,web3`
in `docker-compose.chiado-node.yml:156`).

**Follower:**
- `EEZ_SEQUENCER_RPC` (`eez-follower/src/lib.rs:47`) now requires
  `EEZ_SEQUENCER_ADDRESS`; missing it is a startup error.
- The header-only poller (`unsafe_head.rs:122`), a bare FCU via
  `advance_unsafe_head`, is replaced. It polls `eez_getSignedHead` every L2
  block time and runs the fork choice.
- `advance_unsafe_head` stays, because #206's stateful backend calls it.
- The exclusion with the stateful prover (`lib.rs:70-75`) is unchanged.

**Tests:**
- **Unit:** sign/verify round trip, wrong signer, tampered field, block-hash
  mismatch, version ordering across restarts and sources, replay,
  equivocation, future epoch, failover when the preferred source's chain
  stops descending from safe, and the decision table.
- **e2e, `happy_case_follower_sequencer_rpc`** (`e2e.rs:969`): assert the
  follower's head reaches the sequencer's signed head while above L1 safe.
  This replaces the log-line check.
- **e2e, rogue-sequencer test** (`e2e.rs:1122`): a follower configured with
  the wrong address adopts no unsafe head, and safe still holds.
- **e2e, replacement** (new): drive a failed bundle and assert the follower
  adopts the empty sibling before L1 settles it. Candidate levers, for the
  builder to pick:
  - draining the poster with `anvil_setBalance`;
  - the staging poster;
  - an L1 reorg via `Chain::reorg` (`eez-testkit/src/lib.rs:206`).

**Budget:** about 1.4k lines.

## p2p-v0 (PR 2)

**Overlay (consensus layer).** Crate `eez-p2p`: libp2p 0.56 gossipsub over
TCP/Noise/Yamux. The transport setup is reused from the closed #119. 0.56
rather than 0.57 keeps libp2p-identity and multiaddr at the versions reth's
discv5 uses.

- **Switch:** a node joins the overlay when `EEZ_P2P_LISTEN`, a TCP
  multiaddr, is set. A composer with `EEZ_P2P_LISTEN` but no
  `EEZ_SEQUENCER_KEY` refuses to start.
- **Identity:** the overlay uses the node's reth secp256k1 key, the same one
  as its devp2p identity. Each node therefore has one identity, and the peer
  id follows from the ENR public key.
- **Discovery:** through reth's discv5.
  - Each node adds an `eez` entry, the RLP of its overlay TCP port, to its
    ENR at network build time, through a custom network builder.
  - Every 5 s the overlay reads the discv5 table, and dials ENRs that carry
    the entry and our fork id.
  - `EEZ_P2P_PEERS` adds static `/…/p2p/<id>` peers, for nodes that run with
    discovery off, e.g. a sequencer behind relays.
- **Publishing:** the composer publishes from the signed-head store's
  `watch`. It accepts heads signed by its own key, since several composers
  may share it. Every node relays.
- **Topic:** `/eez/<l2 chain id>/<rollup id>/heads/0`.
- **Message:** raw Snappy of the RLP envelope `[head, signature, block]`,
  where an empty `block` means none, capped at 10 MiB.
- **Authentication:** gossipsub runs anonymous (`ValidationMode::Anonymous`),
  because the sequencer signature is the authentication. The message id is
  the SHA-256 of the data.
- **Relay rule:** gossipsub forwards a message only after the node reports a
  verdict (`validate_messages`):
  - **Accept:** a valid envelope from the authorized address whose counter is
    above its source's latest. Every source's heads propagate, not only the
    preferred one's.
  - **Ignore:** a head at or below its source's latest, or a future epoch.
    - A head the node has already seen is ignored however it is encoded.
    - Snappy has many encodings of one payload, so accepting copies would let
      a peer flood the mesh with re-encodings of one valid head.
    - The overlay keeps its own tracker, so this never hides a head the
      follower learned over RPC.
  - **Reject:** undecodable, the wrong signer, or an equivocation. v0 does not
    score peers, so Reject only stops the message; penalties come with peer
    scoring.
- **Never blocking:** accepted heads reach the follower through a bounded
  channel with `try_send`; overflow is dropped and WARNed. The swarm loop
  never awaits a channel.

**Block fetch (execution layer).** L2 reth nodes find each other over discv5
with our own `--bootnodes`. Discovered peers open eth sessions with each
other.

- **Mainnet fallback guard:** `node_cli` refuses to start with discovery on
  and no `--bootnodes`. Otherwise reth falls back to Ethereum mainnet's
  bootnodes for a custom chain.
- **Discovery off:** `--trusted-peers` still works.
- **Transaction gossip:** every L2 node runs with `--disable-tx-gossip`, which
  `node_cli` adds itself.
  - A transaction enters a node only through that node's own RPC.
  - Cross-chain transactions enter only through the composer's fronts.
  - So a transaction sent to a follower never reaches the sequencer. How
    followers accept transactions is still open.
- **Fetching:** with the overlay on, a follower fetches missing blocks only
  from peers, through reth's `FetchClient`. It checks the header hash and the
  body against the header. The RPC block source exists only in RPC mode.
- **Peers:** a fetch runs only when `num_connected_peers() > 0`, and it is
  time-boxed, because reth queues requests forever without peers.
- **`advance_unsafe_head`** (used only by #206's stateful backend) still
  accepts SYNCING. With devp2p peers, reth could then sync an unknown target
  itself while the committer's head stays stale. #206's integration owns
  that call.

**Follower.** Heads arrive from gossip and from the RPC poller. Both feed the
same tracker, and the highest version wins.

**Configs.** The testkit gains per-node extra CLI args and leased devp2p and
discv5 ports. Deployment configs have no L2 follower service yet, so they
don't change.

**Multi-client.** Signatures are checked in our consensus layer, which drives
the execution client over the engine API. Block data uses the standard eth
protocol, and discovery is standard discv5. Nethermind or Erigon would run as
execution clients behind an eez node, whose own discv5 carries the same `eez`
entry. They would need eez block support, such as the native `0x76` system
transaction, but no part of this overlay.

**Tests:**
- **Unit:** codec, verdicts.
- **Crate integration** (static peers):
  - heads relay through a middle node that never drains its inbound channel;
  - a rogue flood doesn't stall the publisher;
  - a peer started late is dialed.
- **e2e:** three nodes find each other from the seed's bootnode alone.
  Follower B has no RPC, starts late, and fetches its missing ancestors from
  peers.

**Budget:** about 1.4k lines.

## Follower modes

The stateful prover is switched on by `EEZ_STATEFUL_PROOF_SIGNER_ADDR`
(`eez-prover-stateful/src/config.rs:26`). Its backend imports streamed blocks
and moves the unsafe head itself (#206), so it must be the only unsafe writer.

| Version | Head follower switch | Sources | Both on (with the stateful prover) |
|---|---|---|---|
| main today | `EEZ_SEQUENCER_RPC` | the RPC | startup error (`eez-follower/src/lib.rs:70-75`) |
| seq-sig-v0 | `EEZ_SEQUENCER_RPC`; it now also requires `EEZ_SEQUENCER_ADDRESS` | the RPC | startup error, as before |
| p2p-v0 | `EEZ_SEQUENCER_ADDRESS` | `EEZ_SEQUENCER_RPC` and/or `EEZ_P2P_LISTEN` | startup error |

Startup rules:
- **Neither switch:** an L1-derived-only follower.
- **In p2p-v0:**
  - an address without a source is a startup error;
  - a source without an address is a startup error;
  - a stateful-prover follower doesn't join the overlay.

## sync-v0 (PR 3)

L1 does not keep data. Blobs expire after about two weeks, and L1 clients
prune old history (EIP-4444), so even calldata goes away. Blob archives are out
of scope. Every node except a new chain's first composer therefore gets its
history from peers.

L1 stays the only trust anchor. `rollups(id).root` is the L2 block hash of the
last settled block, and a block hash pins its whole ancestry. Peers can
withhold history but not forge it.

Citations in this section are to `feat/p2p-v0` at `5108818d`; reth citations
are to `fa9e76a`.

**Modes.**
- **Default:** history from peers. A node that needs history and has no peers
  waits, logging its status. It never falls back to L1 on its own.
- **`EEZ_SYNC_L1_ONLY=1`:** today's derivation from L1, from the boot
  checkpoint or else the deploy block. It is for a new chain's first composer,
  or a node that re-verifies everything from L1. When L1 can't serve a block it
  needs, it fails with "L1 history incomplete" instead of retrying forever.
  Once its scan passes L1-finalized, the local chain must hold that block's
  `root`.

**Steady state is unchanged.**
- Unsafe blocks come from gossip and p2p-v0's tail fetcher.
- The Deriver confirms each L1 settlement. It skips when the local terminal
  block equals L1's settled hash, and replays L1's version otherwise.
- A head is adopted only if it descends from safe.

**Boot, both binaries, default mode.** Nothing else sends forkchoice updates
until step 6 ends.

1. **Read the anchor.**
   - `F` is L1's finalized block, number and hash.
   - `H_F` is `rollups(EEZ_ROLLUP_ID).root`, read at `F`'s hash (EIP-1898), so
     both come from the same block.
   - If L1 has no finalized block yet, or the RPC can't serve state at `F`:
     wait, with a status line naming the cause.
   - If the rollup isn't registered at `F` (no registry code, or
     `rollupContract == 0`): seed at L2 genesis, as (`F`, block 0). No post can
     predate registration.
   - If it is registered with root 0: the deployment is misconfigured; fail.
2. **If the local canonical chain holds `H_F`,** seed at `F`. This is the
   normal restart.
3. **Otherwise, sync to `H_F`:**
   1. Rewind to the node's own finalized block `f`, reth's persisted finalized
      (genesis on a fresh node). Any local tail above it goes, so reth syncs
      forward from a block L1 finalized.
   2. Call `sync_to(H_F)`: an FCU with head = safe = finalized = `H_F`,
      repeated every 2 s until VALID. reth backfills with its pipeline when the
      gap is over 32 blocks, and downloads the range otherwise
      (`crates/engine/tree/src/tree/mod.rs:2768-2840`,
      `crates/engine/primitives/src/config.rs:28`). While it answers SYNCING,
      log a WARN every 30 s with the connected peers, the local best block,
      `F` and `H_F`.
   3. After VALID, `f`'s parent must still be canonical. Otherwise fail loudly:
      the datadir is not on L1's chain (a wrong deployment or a wrong L1). The
      parent, not `f`: a resume can legitimately replace `f` with an
      L1-settled sibling at the same height.
   4. Re-read `F` and repeat until `H_F` is local. A long sync then ends with a
      short top-up instead of a long window to replay from L1.
4. **Seed the Deriver** with one synthetic index record:
   - post (`F`, hash of `F`, log index `u64::MAX`), last L2 block = height of
     `H_F`;
   - every post at or before `F` reads as processed
     (`l1_canonical_head.rs:82-88`); the boot-checkpoint seed is the template
     (`deriver.rs:388-401`);
   - safe and finalized start at `H_F`, so reth's finalized never resets to
     genesis after a restart;
   - the record is a floor. An L1 reorg or an index revalidation below it is
     fatal (finality broke, or the RPC lied); it never rewinds L2.
5. **Reconcile `F` → L1 head** with the existing scan and its retry/terminal
   split.
   - The window is recent: about 13–19 min on Ethereum and 3–4 min on Gnosis,
     so its DA is still on L1.
   - No post straddles `F`: deferred entries settle in the L1 block that
     verified the batch (`EEZ.sol:1579`).
6. **Composers only: drop the unsettled tail** (below).
7. **Open the gate:**
   - the Deriver loop, then the stateful prover;
   - followers: the head follower and the overlay;
   - composers: the sequencer, the head signer and the overlay;
   - the L1 watcher, from `F`.

   The composer's head signer starts here, not before boot
   (`eez-node/src/lib.rs:361-394`). Otherwise a syncing composer signs stale
   heads under the newest epoch, which followers prefer.

Default mode neither reads nor writes the boot checkpoint, and never scans from
the deploy block. A checkpoint over peer-synced history would let a later
L1-only boot skip verifying it. L1 readiness probes `F`, not the deploy block
(`eez-node-common/src/lib.rs:177`).

**Composer restart: drop the unsettled tail.** Today a composer restarted with
a rich Sync block above L1 safe never posts again. The optimistic ledger lives
in memory, and emission refuses a range with system transactions in an
intermediate block (`composer.rs:4254-4285`; the ignored test
`durability.rs:19-20`).

After step 5, the composer rewinds to L1 safe before the sequencer starts,
dropping every block L1 has not settled.
- **Every transaction in the dropped blocks is lost.**
  - reth's rewind to an ancestor doesn't notify the transaction pool, so
    nothing is re-added (`mod.rs:985-1004`, `:1068-1084`).
  - Cross-chain transactions lived only in the held pool, which didn't survive
    the restart.
  - The composer logs each dropped hash at WARN, and users resubmit. Persisting
    the held pool is a separate effort.
- **A postBatch in flight from before the restart** may still land. The Deriver
  then replays L1's version over the composer's new blocks, as for any
  competing batch.

The same rewind loses pure L2 transactions in steady state, when a failed
bundle is rolled back. That is fixed on its own branch off main, not here.

**Block import.**
- **Boot (step 3):** reth's own sync. reth makes a synced head canonical before
  it checks safe and finalized, and a failed check is only a debug log
  (`mod.rs:2002-2034`). That is harmless only when head, safe and finalized are
  the same block.
- **Unsafe tail:** p2p-v0's fetcher stays. It checks descent from safe before
  import, and it imports under the reconcile lock.
  - One change: the `last_indexed_l2() > safe` hold (`unsafe_head.rs:433-448`)
    is also checked before fetching (`:407`). A held follower then stops
    downloading blocks it would discard.
  - The check under the lock stays authoritative, since the Deriver can index a
    batch in between.

**BlockCommitter.**
- **New `sync_to(hash)`:** sends FCU(hash, hash, hash) and changes no state.
  - SYNCING returns Syncing. The target is not kept for later FCUs.
  - After VALID, `adopt_synced(header)` sets the head, safe and finalized
    mirrors to that block without another FCU; reth is already there.
- **Boot waits for an idle engine.** A node killed mid-backfill restarts with
  reth resuming it, and reth answers SYNCING to every FCU until it finishes.
  Boot repeats a plain forkchoice refresh until reth is idle, logging a WARN,
  before it sends anything else.
- **SYNCING never counts as success** for a command that moves safe or
  finalized. `advance_safe_finalized` stores its cursors only on VALID; today
  it accepts SYNCING (`block_committer.rs:655-659`).
- It builds on #206's "engine not ready" outcome. #206 merges before this
  stack.

**Peers and history.**
- **Peers:** every node that may need history has devp2p peers, either
  `--trusted-peers` with the composer's enode (a stable node key), or
  `--bootnodes` with discovery on.
- **History nodes** keep full block history. That rules out `--minimal`, which
  prunes bodies older than about 10k blocks.
  - A network needs at least two independent history nodes.
  - On a small network the composer is often the only one, and serving syncs
    competes with block production.
- **Configs:**
  - the genesis composer runs `EEZ_SYNC_L1_ONLY=1`;
  - every other L2 node gets trusted peers: node B, followers, and #206's
    stateful prover;
  - compose files and kurtosis change accordingly.

**Tests.**
- **Spike first.** reth's pipeline has never run on our blocks, which carry
  native `0x76` system transactions and use a custom executor. Two nodes: B
  boots more than 32 blocks behind, with the composer as its trusted peer, and
  must reach VALID with matching hashes.
- **e2e `fresh_follower_syncs_history_from_peers`:**
  - Anvil is set to finalize quickly (`--slots-in-an-epoch`).
  - A follower with the composer as trusted peer syncs to `H_F`, and its first
    scan starts at `F`, not at the deploy block.
  - Negative control: a follower without peers keeps waiting at genesis.
- **Durability:** the `durability.rs` restart test is un-ignored, and the
  restarted composer settles again.
- **Explicit modes:**
  - Every e2e node sets its mode explicitly. Anvil finalizes 64 blocks behind
    its head, so a node booting more than about 256 s after deploy would
    otherwise switch paths.
  - `deep_backfill_late_join` runs with `EEZ_SYNC_L1_ONLY=1`.
- **Unit:**
  - the boot decision (steps 1–3);
  - the seed and floor in `L1CanonicalHead`;
  - `sync_to` outcomes;
  - the L1-only "history incomplete" error.

**Known limits.**
- **A resume at `H_F`'s height after the seed is refused.** It appends to that
  Sync block, and it is already refused once that height is finalized
  (`deriver.rs:1719-1731`). It gets its own error instead of a resync loop.
- **Blob DA**, when it comes, needs a beacon endpoint for the window.
- **Old history's DA isn't re-checked:** new nodes don't re-check it, because
  v0 trusts L1 settlements.

**Budget:** about 1.3k lines, tests included.

## Decisions

**Owner:**
- v0 trusts L1 settlements unconditionally.
- Signatures exist only on the overlay, never in the DA.
- seq-sig-v0 and p2p-v0 are separate stacked PRs.
- Ordering is by an ephemeral version, not a persisted counter.
- Several composers may sign with one shared key. A follower follows one of
  them, and L1 still decides settled blocks.
- Startup sync mixes L1 and peers.
- Branches are cut from main.
- This doc stands alone.

**Mine:**
- EIP-712 for the message.
- The 10 s future bound.
- `eez_getRawBlock` instead of the `debug` namespace.
- The crate name.
- The topic string.
- `watch` instead of a broadcast channel.
- Fork-choice candidates are only heads that descend from safe, which gives
  failover between composers from L1 facts instead of timers.
- Version order is per source, and the newest epoch is preferred.
- The relay rule accepts an exact copy of a source's latest, and a future
  epoch is Ignored rather than Rejected.
- The head follower is enabled by address plus a source, and the stateful
  exclusion keys on it.
- Nethermind/Erigon as execution clients behind an eez node.

**Owner, for p2p-v0:**
- Overlay discovery runs through reth's discv5.
- There is no RPC fallback for blocks in p2p mode.
- Discovery is on.
- Sequencer promote/demote is manual, in its own later PR.

**Mine, for p2p-v0:**
- Anonymous gossipsub, with the sequencer signature as the only
  authentication.
- Reject without peer penalties until peer scoring lands.
- `EEZ_P2P_LISTEN` as the overlay switch, and a startup error for a composer
  that sets it without a key.
- The reth node key as the overlay identity.
- `--disable-tx-gossip` forced, and the mainnet-bootnode fallback refused, by
  `node_cli`.
- Deployment configs unchanged until an L2 follower service exists.
- Discovered peers are dialed only while fewer than 16 are connected.
- Inbound overlay connections are capped at 64, two per peer.

**Owner, for sync-v0:**
- History comes from peers by default. L1-only derivation runs only with
  `EEZ_SYNC_L1_ONLY`.
- No automatic fallback: a node without peers waits.
- Long-range sync uses reth's own sync, triggered by an FCU.
- One boot path for every boot, anchored at L1-finalized.
- A restarted composer drops its unsettled tail and the tail's cross-chain
  transactions. Persisting the held pool is a separate effort.
- #206 merges before this stack.

**Mine, for sync-v0:**
- The unsafe tail stays on p2p-v0's fetcher, because reth checks safe only
  after it canonicalizes a synced head.
- "Wrong chain" means the parent of the node's own finalized block is gone
  after the sync, not a different block at `H_F`'s height. A replaced or
  partially settled tail is a normal restart.
- The synthetic seed record, and the floor.
- Rewind to the node's own finalized block before syncing, and re-read `F`
  until `H_F` is local.
- Default mode doesn't write checkpoints, and L1 readiness probes `F`.
- The composer's head signer starts after the gate.
- On restart, the composer drops every block above L1 safe, and logs the
  hashes of the transactions lost with them.
- FCU every 2 s and a WARN status every 30 s while syncing.

**Left to the builder:**
- Module layout inside the crates.
- The RPC poll cadence, if it differs from the L2 block time.
- Metric names.
- Which e2e lever drives the replacement test.
