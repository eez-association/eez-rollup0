# Sequencer heads and P2P: specification v0

This spec defines how an EEZ L2 sequencer signs its unsafe head, how nodes
carry signed heads to each other, and how a node gets L2 blocks and history
from its peers. A client that follows it can:

- follow the sequencer's signed heads, or publish its own as a sequencer;
- relay heads on the overlay;
- peer with other L2 execution clients over devp2p to fetch blocks and sync
  history.

It does not cover L1 derivation, settlement or the block format. Those are in
[`l1-batch-scan.md`](l1-batch-scan.md) and
[`native-system-transactions.md`](native-system-transactions.md).

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are used as in
RFC 2119. Byte strings are hex. RLP is Ethereum's recursive length prefix:
integers are big-endian with no leading zeros, and zero is the empty string
`0x80`. Decoding is strict, except for typed transactions inside a block
([§4.1](#41-envelope)). Each of these is invalid:
- a non-canonical integer, such as one with leading zeros or `0x8105`;
- a non-canonical length prefix;
- an integer wider than its field;
- a list with missing or extra elements.

The reference implementation is eez-node ([Appendix B](#appendix-b-reference-implementation)).
Where this spec describes what the reference accepts rather than what a
sender produces, it says so. A client MUST accept the same inputs, so that
nodes reach the same verdict on the same message. This matters because:
- nodes relay only the first copy of each claim ([§5](#5-validation));
- the signature doesn't cover the message bytes, so any relay can re-encode
  a message.

A client that rejects a copy the reference accepts can miss that head
entirely.

## 1. Model

```text
finalized  <- L2 block hash stored by L1, as of L1's finalized block
safe       <- L2 block hash stored by L1, as of L1's latest block
unsafe     <- newest sequencer-signed head that descends from safe
```

- L1 alone decides safe and finalized. A signed head never moves either of
  them.
- Safe can move back when L1 reorgs. Finalized never moves back, and a node
  never reorgs below it; see the known limit in
  [§11](#11-joining-history-from-peers).
- The signature proves who produced a head. It is not a preconfirmation, and
  it never reaches L1, the DA or the prover.
- A block hash commits to every ancestor through parent hashes. A peer can
  withhold blocks but can't forge them.

There are two networks:

| Network | Protocol | Carries |
|---|---|---|
| Head overlay | libp2p gossipsub | signed heads, each usually with its block |
| Execution layer | devp2p (RLPx, `eth`) | blocks by hash, history |

Both find peers through one discv5 ENR ([§7](#7-discovery)).

## 2. Parameters

Per network:

| Name | Meaning |
|---|---|
| `L2_CHAIN_ID` | the L2 chain id |
| `REGISTRY` | address of the EEZ registry contract on L1 |
| `ROLLUP_ID` | this rollup's id in the registry (`uint64`) |
| `SEQUENCER` | the one address authorized to sign heads (v0) |
| `L2_BLOCK_TIME` | the L2 block time |

Constants:

| Name | Value |
|---|---|
| `MAX_MESSAGE_SIZE` | 10 MiB (10 485 760 bytes) |
| `MAX_FUTURE_MS` | 10 000 |
| `MAX_SOURCES` | 8 |
| `MAX_ANCESTRY_WALK` | 1024 blocks |
| `TOPIC` | `/eez/<L2_CHAIN_ID>/<ROLLUP_ID>/heads/0`, both in decimal |

## 3. Signed heads

### 3.1 The claim

| Field | Type | Meaning |
|---|---|---|
| `rollupId` | `uint64` | `ROLLUP_ID` |
| `number` | `uint64` | head block number |
| `blockHash` | `bytes32` | head block hash |
| `parentHash` | `bytes32` | the head block's parent hash |
| `epochMs` | `uint64` | the signing process's start time, Unix ms |
| `counter` | `uint64` | the head's position in that process's sequence |

### 3.2 Signing hash

The claim is signed as EIP-712 typed data.

```text
EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)
  name              "EEZ Sequencer"
  version           "0"
  chainId           L2_CHAIN_ID
  verifyingContract REGISTRY

SignedHead(uint64 rollupId,uint64 number,bytes32 blockHash,bytes32 parentHash,uint64 epochMs,uint64 counter)
```

- The domain has no salt.
- The `SignedHead` typehash is
  `0x92ace2e10d606aabd1dbb446b77fb1b2c97c195ad066f0600490e54048d98051`.
- The signing hash is
  `keccak256(0x1901 ‖ domainSeparator ‖ hashStruct(SignedHead))`.

`REGISTRY` in the domain separates two deployments on one L1 that share an
L2 chain id. The domain doesn't name the L1 chain, though. Deployments on
different L1s with the same `L2_CHAIN_ID`, `REGISTRY` and `ROLLUP_ID` share a
domain and a topic. They MUST NOT share a sequencer key, or a head signed for
one is valid on the other.

### 3.3 Signature encoding

The signature is 65 bytes: `r ‖ s ‖ v`, with `r` and `s` as 32-byte
big-endian integers.

A signer MUST produce `v ∈ {27, 28}` (27 + y-parity) and `s ≤ n/2`, where `n`
is the secp256k1 group order.

A receiver in v0 accepts exactly what the reference accepts:
- the length MUST be exactly 65 bytes;
- `v` MUST be one of:
  - 0 or 1, giving parity `v`;
  - 27 or 28, giving parity `v − 27`;
  - 35 to 255, giving parity `(v − 35) mod 2`;
- an `s` above `n/2` is accepted: the receiver replaces it with `n − s` and
  flips the parity before recovery;
- `r` and `s` MUST be valid non-zero scalars, and recovery MUST succeed;
- the signer is the address recovered from the signing hash.

A coming version restricts receivers to the signer rule (`v ∈ {27, 28}` and
low `s`). Signers that follow the rule today stay valid.

### 3.4 Versions and sources

- A head's **version** is `(epochMs, counter)`, compared in that order.
- A **source** is one signing process, identified by `(signer, epochMs)`.
  Several sequencers MAY share one key; each runs as its own source. They
  MUST use distinct `epochMs` values. Two that start in the same millisecond
  form one source, and their differing heads are equivocations.

A signer:
- MUST set `epochMs` to its wall-clock start time in milliseconds, and MUST
  use a larger `epochMs` after every restart;
- MUST start `counter` at 1 and add 1 for every head it signs;
- MUST NOT sign two different `blockHash` values with one version.

Within a source the higher counter is newer. Across sources the higher
`epochMs` wins, so followers prefer the most recently started sequencer.

## 4. Message format

### 4.1 Envelope

```text
envelope = [head, signature, block]
head     = [rollupId, number, blockHash, parentHash, epochMs, counter]
```

- `rollupId`, `number`, `epochMs` and `counter` are RLP integers.
- `blockHash` and `parentHash` are 32-byte strings.
- `signature` is a byte string (65 bytes; see [§3.3](#33-signature-encoding)).
- `block` is a byte string. When present, it holds the block's consensus
  encoding, wrapped once more as an RLP string. The empty string means no
  block. The encoding is `rlp([header, transactions, ommers])`, plus
  `withdrawals` as a fourth element exactly when the header has a
  `withdrawalsRoot`. Receivers MUST accept both forms. As in Ethereum block
  bodies:
  - a legacy transaction is an RLP list;
  - a typed transaction, `0x76` included, is an RLP string holding
    `type ‖ payload`. Senders MUST wrap it this way with the exact length.
    Receivers MUST also accept the forms the reference accepts:
    - the bare `type ‖ payload` bytes, with no wrapper;
    - a wrapper whose declared length differs from the transaction's, as
      long as at least that many bytes remain in the transactions list. The
      wrapper's header must still be canonical, so `0x81` before the type
      byte is invalid.

    The reference reads the string header, then decodes the transaction from
    the type byte, ignoring the declared length (alloy keeps this for
    backwards compatibility). The transactions root is computed from the
    decoded transactions, so such a copy has the same header hash.

  The header is checked only for shape here, not against the L2 fork
  schedule, which its genesis sets:
  - It has the 15 base Ethereum header fields, then any prefix of
    `baseFeePerGas`, `withdrawalsRoot`, `blobGasUsed`, `excessBlobGas`,
    `parentBeaconBlockRoot`, `requestsHash`, `blockAccessListHash` and
    `slotNumber`.
  - `number`, `gasLimit`, `gasUsed`, `timestamp`, `baseFeePerGas`,
    `blobGasUsed`, `excessBlobGas` and `slotNumber` are 64-bit; `difficulty`
    is 256-bit; `nonce` is 8 bytes.

  Each transaction MUST decode as one of these:
  - a legacy transaction;
  - type `0x01`, `0x02` or `0x04`;
  - type `0x03`, without its sidecar. It decodes here, but block execution
    rejects it;
  - type `0x76`, with payload `rlp([chainId, nonce, to, value, input])`
    ([`native-system-transactions.md`](native-system-transactions.md)). A
    `to` other than EEZL2 (`0x4200000000000000000000000000000000000007`)
    fails to decode.

### 4.2 Message

A gossip message is the envelope's RLP, compressed with Snappy in the raw
block format, not the framed format.

- A sender MUST keep the envelope's RLP within `MAX_MESSAGE_SIZE`, and the
  compressed message too.
- A receiver MUST read the uncompressed length from the Snappy preamble and
  reject anything over `MAX_MESSAGE_SIZE` before decompressing. Its only
  other size limit is the gossipsub frame cap ([§6.3](#63-gossipsub)), so it
  accepts a compressed message slightly over `MAX_MESSAGE_SIZE`.
- A receiver MUST reject trailing bytes after the envelope.
- Snappy allows many encodings of one input. A receiver MUST accept any valid
  encoding. Different encoders produce different bytes and so different
  message ids ([§6.3](#63-gossipsub)).
- The preamble is a little-endian base-128 varint.
  - Senders MUST write it minimally.
  - The reference reads up to 10 bytes and accepts redundant continuation
    bytes. It silently drops bits above 64. The value must then be at most
    `MAX_MESSAGE_SIZE`, and equal the decompressed length.
  - Receivers MUST accept the same, though some Snappy libraries reject a
    preamble longer than 5 bytes.

A signer SHOULD attach the block. The reference always attaches it, rewinds
included. A receiver MUST accept an envelope without one.

## 5. Validation

A node checks every received message in this order. The first failing step
decides the verdict.

1. **Decode** the message ([§4.2](#42-message)). Failure: Reject.
2. **Rollup:** `rollupId` MUST equal `ROLLUP_ID`. Otherwise Reject.
3. **Signer:** recover the signer ([§3.3](#33-signature-encoding)). It MUST be
   `SEQUENCER`. A malformed signature or the wrong signer: Reject.
4. **Block**, when present:
   - it decodes as one EEZ block with no trailing bytes
     ([§4.1](#41-envelope)). An unknown transaction type or a malformed
     transaction fails;
   - its body matches its header:
     - `keccak256(rlp(ommers))` equals `ommersHash`;
     - the transactions trie root equals `transactionsRoot`;
     - the withdrawals root equals `withdrawalsRoot`, and withdrawals are
       present exactly when the header has a `withdrawalsRoot`;
   - `keccak256(rlp(header))` equals `blockHash`. Strict decoding makes the
     received header bytes and their re-encoding identical;
   - the header's `parentHash` and `number` equal the claim's.

   Any failure: Reject.
5. **Freshness**, against the latest accepted head of the claim's source:

   | Condition | Verdict | Effect |
   |---|---|---|
   | `epochMs > now_ms + MAX_FUTURE_MS` | Ignore | not remembered |
   | first head of its source | Accept | remembered |
   | `counter` below the source's latest | Ignore | none |
   | same `counter`, same `blockHash` | Ignore | none (duplicate) |
   | same `counter`, different `blockHash` | Reject | the first is kept (equivocation) |
   | `counter` above the source's latest | Accept | replaces the latest |

A node remembers at most `MAX_SOURCES` sources. When a new source would
exceed that, the node evicts the source with the lowest `(epochMs, signer)`.
If that is the new source itself, the verdict is Ignore.

Duplicates are detected by claim, not by bytes. A re-encoded copy of a seen
head is therefore ignored. So is a copy whose signature has another accepted
encoding.

- `now_ms` is the local wall clock.
- A source's latest head is kept in memory only. After a restart, the first
  head of each source is accepted again.
- A head with the same counter and `blockHash` as its source's latest is a
  duplicate, whatever its other fields.
- `number` and `parentHash` are checked only against an attached block.
  Without one, the follower uses the claim's values for its checks in
  [§10](#10-follower-fork-choice). It imports the block fetched by
  `blockHash` even if its number or parent differs from the claim.

## 6. Head overlay

### 6.1 Transport

- TCP. Addresses learned from discovery are IPv4. Static peers and the
  listener MAY be IPv6.
- Security: Noise (`/noise`), per the libp2p Noise spec.
- Multiplexing: Yamux (`/yamux/1.0.0`).
- The reference speaks no other transport (no QUIC or WebSocket) and dials
  no DNS addresses.

### 6.2 Identity

A node's overlay identity is its secp256k1 node key, the same key as its
discv5 ENR and devp2p identity. Peers derive the libp2p peer id from the ENR
public key ([libp2p peer ids](https://github.com/libp2p/specs/blob/master/peer-ids/peer-ids.md),
secp256k1, compressed). A node whose overlay key differs from its ENR key
can't be dialed from discovery.

### 6.3 Gossipsub

- Protocol ids: `/meshsub/1.2.0`, `/meshsub/1.1.0` and `/meshsub/1.0.0`.
- One topic, `TOPIC`. The topic id is the string itself, not a hash of it.
- Messages are anonymous, and the sequencer signature is the only
  authentication:
  - publishers MUST omit `from`, `seqno`, `signature` and `key`;
  - receivers MUST reject a message that carries `from`, `seqno` or
    `signature`. A field that is present but zero-length counts as carried;
  - such a rejected message MUST NOT enter the seen-message cache, so it
    can't suppress a clean copy with the same id;
  - the reference accepts, and relays, a message that carries only `key`.
    Ethereum consensus's `StrictNoSign` would reject it.

  A receiver MUST NOT reject on `key` alone. The message id ignores `key`, so
  a rejected keyed copy would share its id with the honest message.
- The message id is `SHA-256(data)`, the full 32 bytes, computed over the
  compressed message.
  - This differs from Ethereum consensus, which hashes a domain plus the
    decompressed bytes and keeps 20 bytes.
  - Two encoders can give one head two ids, so `IHAVE`, `IWANT` and
    `IDONTWANT` may carry it twice. [§5](#5-validation) still ignores the
    second copy.
- A node MUST validate a message ([§5](#5-validation)) before forwarding it,
  and report the verdict: Accept forwards, Ignore and Reject don't. v0 doesn't
  score peers, so Reject only stops the message.
- The largest RPC frame is `MAX_MESSAGE_SIZE + 65 536` bytes.
- Every node subscribes to the topic and relays. A sequencer accepts heads
  signed with its own key, since other sequencers may share it.
- The reference runs gossipsub only, with no `identify` or `ping`. Peers MUST
  NOT require either.

The reference uses the rust-libp2p defaults, which are RECOMMENDED:

| Parameter | Value |
|---|---|
| D, D_lo, D_hi, D_out | 6, 5, 12, 2 |
| D_lazy, gossip factor | 6, 0.25 |
| heartbeat interval | 1 s |
| message cache: history, gossip windows | 5, 3 |
| fanout TTL | 60 s |
| seen-message cache | 60 s |
| prune backoff | 60 s |
| flood publish | on |

Connection limits in the reference are informative:
- discovered peers are dialed only while fewer than 16 are connected;
- at most 64 inbound connections, and two per peer;
- a connection with no open substream closes after 60 s.

## 7. Discovery

Nodes find each other with discv5. A node in the overlay MUST put these
entries in its ENR:

| Key | Value |
|---|---|
| `ip` | IPv4 address |
| `eth` | the EIP-2124 fork id as `rlp([[forkHash, forkNext]])`, as in the `eth` Status message |
| `eez` | the overlay TCP port, as an RLP integer (encoded like `tcp`) |

- A node dials an ENR on the overlay only if it carries `eez`, `ip` and an
  `eth` fork id equal to its own. Equal, not merely EIP-2124 compatible: a
  node that has scheduled a fork the other hasn't isn't dialed until both
  agree.
- The address it dials is `/ip4/<ip>/tcp/<eez>/p2p/<peer id from the ENR key>`.
- The `eez` port MUST be the port the overlay listens on. It is advertised
  before the listener binds, so it MUST be fixed and non-zero.
- Nodes MAY also take static overlay peers as multiaddrs ending in
  `/p2p/<peer id>`, for example a sequencer behind relays with discovery off.

Reference nodes:
- take bootnodes as enode URLs, with the discv5 UDP port in `?discport=`;
- run discv4 alongside discv5. Only discv5 carries the `eez` entry.

## 8. Execution-layer networking

L2 execution clients peer over devp2p (RLPx) with the `eth` protocol.

- **Status:**
  - `networkId` is `L2_CHAIN_ID`;
  - the genesis hash and the EIP-2124 fork id come from the L2 genesis;
  - for `eth/66` to `eth/68`, the reference sends total difficulty 0. It
    sends 0 at startup, then the genesis `terminalTotalDifficulty`, which
    is 0 in EEZ genesis files.
- **Versions:** the reference EL (reth) offers `eth/66` to `eth/71`. It
  doesn't offer `snap`. A peer therefore syncs by downloading headers and
  bodies and executing them.
- **No block gossip.** Unsafe blocks travel in envelopes on the overlay. A
  node missing a block fetches it by hash:
  - `GetBlockHeaders` from the hash, in reverse;
  - then `GetBlockBodies`.

  It checks each header's hash and links, and each body against its header
  ([§5](#5-validation), step 4).
- **Serving:** a node MUST serve its canonical headers and bodies by hash.
  Nodes that serve history keep full block history.
  - A peer that has just accepted a head may not have its block yet.
  - The sequencer, and every node that adopted the head, serves it.
- **Transactions:** reference nodes don't gossip transactions.
  - They neither send nor import transaction announcements or broadcasts, and
    they don't disconnect a peer that sends them.
  - A transaction reaches the sequencer only through the sequencer's own RPC.
  - Peers MUST NOT rely on transaction propagation.
- **Blocks:** executing EEZ blocks requires the native `0x76` system
  transaction ([`native-system-transactions.md`](native-system-transactions.md)).

## 9. Sequencer

A sequencer:
- MUST sign every change of its canonical L2 head, including rewinds and
  replacements. Examples: a failed settlement rolled back, a block rebuilt
  after partial settlement, a retreat after an L1 reorg. The reference falls
  short in two cases: a head whose block stays unreadable for 1 s, and a
  head whose read or signing fails. Either stays unsigned until the head
  changes again.
- MAY skip intermediate heads it never exposed. Followers fetch the ancestors
  by hash.
- MUST NOT sign heads until it has caught up at startup. Caught up means it
  has joined ([§11](#11-joining-history-from-peers)) and derived from `F` to
  L1's head. A syncing sequencer would sign stale heads under its new, highest
  `epochMs`.
- Keeps signing when it sees a newer source under its own key. In v0,
  handing over to another sequencer is an operator action.
- MUST publish each signed head on `TOPIC`, and relay other heads like any
  node.
- SHOULD use a dedicated signing key, separate from its L1 proof, posting and
  system keys.

## 10. Follower fork choice

A follower keeps the latest accepted head of each source and derives safe and
finalized from L1 alone. It chooses its unsafe head as follows.

**Candidates** are the latest heads of all sources, ordered by
`(epochMs, counter, signer)`, highest first.

For each candidate in order:

1. **Conflict check.** Skip to the next candidate if the head provably
   doesn't descend from safe:
   - its number is below safe's;
   - its number equals safe's but its hash differs; or
   - walking its locally known ancestry down to safe's height ends at a
     different hash.

   A head that this doesn't prove in conflict goes on to step 2.
2. **Hold.** If the node's L1 derivation has read a settlement above its safe
   head, stop and adopt nothing. Heights L1 has settled MUST NOT be
   rewritten, even before safe reaches them.
3. **Fetch.** Walk back from `blockHash` through parent hashes to the first
   locally known block, fetching unknown blocks by hash
   ([§8](#8-execution-layer-networking)). The walk is at most
   `MAX_ANCESTRY_WALK` blocks. A head more than `MAX_ANCESTRY_WALK` above the
   highest block on the local canonical chain waits for L1 derivation: stop,
   without trying lower candidates. The reference asks any connected peer
   for up to 64 blocks per request, with a 2 s timeout.
4. **Stop on unknown ancestry.** If the ancestry can't be fetched, stop. Don't
   try lower candidates: unknown ancestry is not a conflict, and a lower
   version must not win because a block is missing.
5. **Fork point.** Check the hold again. Then the first locally known block
   MUST descend from safe. A conflict skips to the next candidate; an
   unverifiable result stops.
6. **Import** the fetched blocks in order, executing each, and set the unsafe
   head to the candidate. If the candidate is already local, rewind or
   advance to it. If a block fails to import, stop and retry on the next run.

The ancestry walks in steps 1 and 5 read at most `MAX_ANCESTRY_WALK` local
headers. A walk that runs out, or reaches a block not held locally, proves
nothing: it is unverifiable, not a conflict.

The follower re-runs this on every accepted head and once per
`L2_BLOCK_TIME`.

- **Safe moving back.** After an L1 reorg moves safe back, candidates that
  conflicted with the old safe are checked again.
- **Failover.** A sequencer that stops publishing keeps the highest version.
  It blocks lower candidates until its head provably conflicts with safe.
  That happens once safe reaches its height, or earlier if the follower still
  holds that chain. Once L1 settles another block, the dead chain's blocks
  usually leave the follower and its peers, so its ancestry turns
  unverifiable rather than conflicting. The follower then moves to the next
  source. With no candidate, the unsafe head stays where it is.

## 11. Joining: history from peers

L1 keeps neither blobs nor, under EIP-4444, old calldata. A new node
therefore gets its history from peers, anchored at L1:

1. Read `F`, L1's finalized block (number and hash).
2. Read `H_F`, the `root` from `REGISTRY` at `F`'s hash (EIP-1898,
   `requireCanonical: true`):

   ```solidity
   function rollups(uint64 rollupId) external view
       returns (address rollupContract, bytes32 root, uint256 etherBalance);  // selector 0xef678d27
   ```

   - If `REGISTRY` has no code at `F`, or `rollupContract` is zero, start
     from L2 genesis.
   - A registered rollup with root zero is a misconfiguration. So is a
     return that doesn't decode. Either way, stop.
   - If the RPC fails or can't serve state at `F`, wait and retry.
3. If the local canonical chain holds `H_F`, start there.
4. Otherwise, sync to `H_F` from devp2p peers. For an execution client behind
   the engine API, that is a forkchoice update with head, safe and finalized
   all at `H_F`.
   - While syncing, re-read `F`. A later settlement can replace `H_F`, and
     peers may stop serving the old one.
   - After the sync, the parent of the node's own previous finalized block
     MUST still be canonical. Otherwise the node is on another chain, and
     MUST stop. The check is on the parent, not the block itself, because
     of the known limit below.
5. Treat `H_F` as safe and finalized, and derive from `F` to L1's head.

A peer can't forge history: `H_F` pins every ancestor. A peer can withhold
it, in which case the node waits.

**Known limit:** a later batch can still append to `H_F`'s block after `F`.
A node that booted in that window stops. Finalizing `H_F`'s parent instead
would close this; it is under discussion.

## 12. Security considerations

- **Stolen key:** the thief can equivocate and outrank the sequencer. Followers
  keep the first head of each version and log the conflict, and L1 still
  decides safe.
- **Clocks:** a signer with a fast clock outranks every other source.
  `MAX_FUTURE_MS` bounds how far.
- **Flooding:**
  - sizes are capped before decompression;
  - messages are validated before they are relayed;
  - a node relays only the first copy of each claim, whatever its encoding,
    so re-encodings of a seen head don't spread.

  v0 has no peer scoring.
- **Malleability:** v0 receivers accept several encodings of one signature
  ([§3.3](#33-signature-encoding)), and of a block's typed transactions
  ([§4.1](#41-envelope)). Any relay can produce them, since the signature
  doesn't cover the message bytes. Each is a duplicate of the same claim. A
  coming version rejects the extra signature encodings.
- **Withholding:** with no usable head, a follower still follows L1. Safe
  keeps advancing.

## 13. Versioning

The topic suffix `/0` and the EIP-712 `version` change together on any
incompatible change. Planned changes include:
- the strict signature rule ([§3.3](#33-signature-encoding));
- the signer set read from L1;
- peer scoring;
- multisig signers.

## Appendix A: test vector

Inputs:

| Input | Value |
|---|---|
| private key | `0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80` |
| signer | `0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266` |
| `L2_CHAIN_ID` | 1337 |
| `REGISTRY` | `0x5FbDB2315678afecb367f032d93F642f64180aa3` |
| claim | `rollupId` 1, `number` 7, `blockHash` `0xbb…bb`, `parentHash` `0xaa…aa`, `epochMs` 1 700 000 000 000, `counter` 1 |
| block | none |

Outputs:

| Output | Value |
|---|---|
| domain separator | `0x7bcd13c41deec50e6183399b45bf4a4676869087cef8d68623fe7ff72b7f8f9c` |
| signing hash | `0x876ed57393bba41ad8986fcdf6d080462b817110409e235077e7d1544f83247e` |

Signature (RFC 6979):

```text
5814195620ef379cdaf95e08a52978644c6b5457e238f204541021875ebebf3e
3e59167b938e5bff0d15a010c87b6f0d2523e7cf1e90960b03ac8e86dc0fa658
1b
```

Envelope RLP (148 bytes):

```text
f892 f84c 01 07
  a0 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  a0 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  86 018bcfe56800
  01
b841 5814195620ef379cdaf95e08a52978644c6b5457e238f204541021875ebebf3e
     3e59167b938e5bff0d15a010c87b6f0d2523e7cf1e90960b03ac8e86dc0fa6581b
80
```

Message, as Snappy-compressed by the reference (one valid encoding):

```text
94011cf892f84c0107a0bb7a010004a0aa7a0100f04b86018bcfe5680001b841
5814195620ef379cdaf95e08a52978644c6b5457e238f204541021875ebebf3e
3e59167b938e5bff0d15a010c87b6f0d2523e7cf1e90960b03ac8e86dc0fa658
1b80
```

Its message id is
`65cd4ff00b3116504c276cbe89085cab1f24b74956fb5cd6f3a621b4f7fbac57`.

The test `spec_vector` in `crates/eez-p2p/src/wire.rs` pins these values.

## Appendix B: reference implementation

| Part | Where |
|---|---|
| claim, EIP-712, versions | `crates/eez-seq-sig/src/head.rs` |
| signature and block checks | `crates/eez-seq-sig/src/envelope.rs` |
| freshness rules | `crates/eez-seq-sig/src/tracker.rs` |
| wire format | `crates/eez-p2p/src/wire.rs` |
| verdicts | `crates/eez-p2p/src/validate.rs` |
| gossipsub, transport, dialing | `crates/eez-p2p/src/overlay.rs` |
| ENR entry and dial filter | `crates/eez-node-common/src/discovery.rs` |
| sequencer signing | `crates/eez-node/src/head_signer.rs` |
| fork choice | `crates/eez-follower/src/unsafe_head.rs` |
| joining | `crates/eez-node-common/src/boot.rs` |

Node configuration and wiring are in
[`sequencer-signatures-and-p2p.md`](sequencer-signatures-and-p2p.md).
