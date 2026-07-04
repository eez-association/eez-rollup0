# Reverse control transport (Level 1) — design

**Status:** DRAFT — nothing implemented. One adversarial design-review pass
run (18 findings, folded in): validator now mandatory in reverse mode,
refuse-while-healthy channel policy, eager-fatal listener bind, dead-server
fatal handling, CGNAT/allowlist caveats, corrected ring-horizon and backfill
failure mode, expanded test matrix. Ready for human sign-off on §7.
**Scope:** invert WHO OPENS the TCP connection between composer and prover,
as a per-pair opt-in. The wire protocol (the three gRPC services and every
message) is unchanged. Single-composer-per-proverd is retained; a
multi-composer prover process (per-session state, QoS, prover-owned L1) is
explicitly **out of scope** (a future "Level 3" design).

## 1. Motivation

Today `eez-proverd` dials the composer's control endpoint
(`EEZ_CONTROL_RPC_URL`, default `http://127.0.0.1:50051`), so the
**composer host must accept inbound connections**. Two deployment realities
argue for the inverse option:

- Composer hosts that must stay outbound-only (home routers, CGNAT, no
  admin access to port-forwarding), while the proving machine is the
  stable, reachable endpoint.
- The production direction — one proving service, N composers — reads
  naturally as "composers dial the prover": one reachable machine, and a
  new composer joins with zero network setup on its side. (With this
  Level 1 alone, that shape is served by one proverd instance/port per
  composer on the proving machine; collapsing them into one process is
  Level 3.)

## 2. Threat model (why direction is NOT a soundness question)

Composers are adversarial by assumption (based rollup: anyone may compose).
The prover's soundness never depends on who connects to whom:

- A **validating** prover (`--validator-bin` + `--chain-config` set)
  re-executes every window (`native-validate` against the witness),
  re-derives system-tx structure from the block's own RLP, telescopes state
  roots across chunks, chains block hashes across chunk boundaries, and
  recomputes the settlement `publicInputsHash` byte-for-byte before signing.
  Its signature is then a **validity certificate**, not a statement of
  trust. Invalid input ⇒ no signature (fail-closed). A valid window chosen
  by an adversary is just a legitimate batch — that is the competition
  model, not an attack.
- **CRITICAL — reverse mode assumes a validating prover.** The prover
  binary today accepts a signer WITHOUT a validator (`native-validate`
  optional); in that config the re-execution and the settlement-chain gate
  are skipped (`crates/eez-proverd/src/main.rs:2088,2202`) yet
  `attest_hash` is still set from the composer-supplied calldata
  (`main.rs:2178-2185`) and signed — so a signer-only prover fed by a
  malicious composer would attest a fabricated `newState` and settle an
  invalid state on-chain (`ECDSAProofSystem.verify` is a bare ecrecover).
  This weakness is pre-existing (it is not introduced by the reversal), but
  reverse mode is exactly the untrusted-composer topology where it bites, so
  the validator is **mandatory** here (§4.2 enforces it at startup; §4.3
  lists it as required).
- The composer accepts attestations only after ecrecover against the
  registered attester; forgery is impossible in either direction.

What the connection direction DOES govern is **admission control for
resources**: whose windows the prover spends CPU on, and who can hold its
streams. A listening prover therefore needs an allowlist (see §4.2) — for
availability, not for signature safety. Symmetrically, a dialing composer
exposes its feed only to the peer it chose: a wrong/malicious dial target
can read the (public, L1-derivable) feed, refuse to attest (liveness), or
spam the ProofSink with garbage signatures (bounded CPU; every submission
is ecrecover-checked and rejected) — the same exposure the listener has
today, scoped to one configured peer.

**CGNAT / shared-IP caveat.** §1 motivates this feature partly with
composers behind CGNAT — but source-IP allowlisting is only meaningful when
the composer presents a **stable, non-shared** source address. Behind
CGNAT the composer's address at the prover is the carrier's shared egress
IP, so the allowlist admits every co-tenant of that NAT (each of which can
then trigger the last-wins channel replacement of §4.2 and displace the
real composer), and the same rotating egress pool can get the legitimate
composer *rejected* on a redial. Such deployments MUST front the listener
with a tunnel that gives a stable peer address (e.g. WireGuard, allowlist
the tunnel IP) or use the §8 deferred per-peer auth. Source-IP allowlisting
is availability hygiene for stable-IP peers, not an authentication layer.

Plaintext note: the channel has no TLS (tonic is built without the `tls`
feature). A WAN man-in-the-middle can (a) read the stream (public,
L1-derivable data); (b) corrupt frames — caught by the prover's contiguity
gates or the composer's ecrecover, so corruption is liveness-only; and
(c) **replay/reorder valid captured frames** — these pass ecrecover and the
gates by construction, so they need their own argument: replayed
attestations are content-keyed and `mark_attested` no-ops an already-
resolved or abandoned window (`proof_sink.rs:159-178`,
`posted_windows.rs:226-253`); replayed directives carry only hints and the
frontier advances solely on a content-keyed attestation, so they trigger a
harmless re-verify; injected/reordered feed frames fail the
parent-hash/number contiguity guard (`eez-proverd/src/main.rs:2030-2040`).
Net impact across all three classes is liveness, not soundness. TLS (or a proxy pair)
can be layered later without touching this design.

## 3. Wire & compatibility invariants

- No `.proto` change. `ControlFeed.Subscribe`, `ProverDispatch.Dispatch`
  and `ProofSink.SubmitSlotProof` keep their exact shapes and semantics.
- The composer remains the **gRPC server** on the connection; the prover
  remains the gRPC client. In reverse mode the prover-side channel is built
  over the accepted socket (the h2 client preface still flows
  prover→composer), so tonic on both ends sees a perfectly normal HTTP/2
  session.
- Both binaries keep their current defaults. Old proverd ↔ new node
  (listen mode) and new proverd (dial mode) ↔ old node interoperate
  unchanged. The reverse mode activates only when BOTH sides are configured
  for it — a per-pair, operator-local decision needing no network-wide
  coordination.

## 4. Design

### 4.1 Composer side (`eez-node`)

**Config.** New env `EEZ_CONTROL_DIAL_ADDR=<ip[:port]>` (IP literal only —
DNS is out of scope for now, §7.3; a name is rejected at startup). A bare IP
reuses `EEZ_CONTROL_RPC_PORT` (default 50051) as the port. Mutually
exclusive with `EEZ_CONTROL_RPC_ADDR` (the listen-mode bind interface):
startup bails if both are set. `EEZ_CONTROL_RPC_PORT` is NOT mutually
exclusive — it just supplies the default port in either mode. Empty values
count as unset. Unset ⇒ today's listen mode, byte-identical behavior
(including the eager fatal bind).

**New module `crates/eez-node/src/control_transport.rs`.**

- `ControlIo`: a `TcpStream` wrapper implementing `AsyncRead`/`AsyncWrite`
  by delegation and tonic's `Connected` (yielding the inner
  `TcpConnectInfo`), plus an optional `oneshot::Sender` drop-guard. tonic
  drops the connection IO when the HTTP/2 session ends — dropping the
  guard is the dial loop's redial signal. Listen-mode sockets use the same
  wrapper (guard `None`) so both modes share one incoming-stream item type.
- `ControlTransport::{Listen(std TcpListener), Dial(mpsc Receiver)}` and
  `incoming(transport) -> Pin<Box<dyn Stream<Item = io::Result<ControlIo>> + Send>>`
  consumed by `Server::serve_with_incoming` (generalizing the existing
  `serve_with_incoming(TcpListenerStream)` call — the current code already
  serves this way, not via `serve(addr)`; the listen arm still registers the
  eagerly-bound std listener with the runtime inside the server task,
  exactly as today).
- `dial_loop(addr, tx)`: connect (wrapped in a 10 s
  `tokio::time::timeout` — see below) → `set_nodelay(true)` → send the
  wrapped socket into the server's incoming stream (mpsc capacity 1) →
  await the drop-guard → log + redial. Backoff on failure: 1 s doubling to
  a 30 s cap, **reset to 1 s only after the connection was actually served
  (guard awaited), not merely after `connect()` returned** — see the
  fast-reject hazard below. The loop never gives up: the composer keeps
  sequencing while the prover is away (the existing deferred-post
  timeout/recover cycle is unchanged), and settlement resumes on reconnect.
  Spawned via `task_executor.spawn_critical_task("eez-control-dial", ...)`.
- **Dead-server-task handling (fatal).** If the `mpsc::Sender::send` fails
  because the server task's receiver was dropped (the critical control-feed
  task died — today its only exit path just logs), the dial loop must NOT
  spin silently handing sockets to a dead receiver: that is silent
  settlement death. On send-error the dial loop returns an error that fails
  the node (same eager-fatal treatment as the listen-mode bind), so the
  container restart policy retries. Symmetrically, the server task dying is
  itself made fatal.
- **Fast-reject hazard.** The prover checks its source-IP allowlist
  *after* `accept()`, so a misconfigured allowlist lets the kernel complete
  the TCP handshake (composer `connect()` succeeds) and only then drops the
  socket. If the loop reset backoff on `connect()` success, this becomes a
  tight reconnect spin. Mitigation: reset backoff only once the connection
  has been served for a minimum dwell (e.g. it stayed up >5 s), so a
  connect-then-instant-drop keeps backing off. This makes an
  allowlist/address misconfiguration visible as steady 1→30 s backoff logs
  rather than a hot loop.
- **Single-connection invariant:** the loop sends the next `ControlIo`
  only after the previous one's guard dropped, so at most one live
  connection exists at a time and `mpsc(1)` never blocks the loop in
  steady state.

**Server keepalive.** `http2_keepalive_interval(30 s)` +
`http2_keepalive_timeout(10 s)` on the tonic server builder, both modes. In
dial mode this is what detects a silently-dead peer (prover power loss, no
RST) and tears the connection down so the drop-guard fires and the loop
redials; in listen mode the pings are benign. This is the one
behavior-visible change to the existing listen mode; an old proverd
tolerates h2 pings natively.

**Failure modes.**

| Failure | Behavior |
|---|---|
| Prover unreachable at boot | dial loop retries forever (capped backoff); node runs; L2 pauses after `EEZ_MAX_SPECULATIVE_DEPTH` as today |
| DNS resolution fails | same retry path (resolution happens per dial) |
| Connect hangs (black-holing firewall) | 10 s `timeout` → treated as a failed dial → backoff |
| Connection drops mid-stream | guard fires → redial; prover-side resubscribe replays from ring/checkpoint |
| Connected but prover main loop wedged (native-validate hung) | h2 session looks healthy; deferred posts time out and re-arm; surfaced only by the composer's `deferred post timed out` ERROR + a stalled `settled=` — see Observability |
| Allowlist rejects composer (wrong IP) | accept→drop each dial; composer sees connect-then-instant-close, steady backoff logs; prover logs a dropped-peer warning — cross-reference these two to diagnose |
| Server task dies / receiver dropped | dial-loop send fails → node exits (fatal) → restart policy retries |
| Both `DIAL` and `RPC_ADDR` set | startup bail with explicit message |

**Observability (new failure surface).** Reverse mode adds
connected-but-not-progressing states that no single log line reveals. The
implementation must emit, at minimum: composer-side a log on every
`reverse_connected` / `reverse_closed` with the peer address, and a WARN if
no attestation has advanced the frontier for N settling slots while a
connection is up; prover-side a WARN naming the dropped peer IP on every
allowlist rejection. A wrong-allowlist misconfiguration otherwise looks
like "composer unreachable" on one side and total silence on the other,
which is not diagnosable in <5 min without these lines.

### 4.2 Prover side (`eez-proverd`)

**Config.** New args (clap, env-mirrored like the rest):

- `--control-listen-addr` / `EEZ_CONTROL_LISTEN_ADDR` (`SocketAddr`):
  activates reverse mode; takes precedence over `--control-addr`. The
  listener is bound **eagerly and fatally** at startup (before the main
  loop), mirroring the composer's eager bind: a bind failure exits so the
  restart policy retries, rather than a background task that only logs.
- `--allowed-composer-ips` / `EEZ_ALLOWED_COMPOSER_IPS` (comma-separated
  `IpAddr` list): **required** in reverse mode — the daemon refuses to
  start with an empty list (an unfiltered listener lets anyone burn
  validator CPU). Peers are checked on `accept()`, before any gRPC bytes
  are read. (See §2's CGNAT caveat: this is availability hygiene for
  stable-IP peers, not authentication.)
- **Validator required in reverse mode:** the daemon bails at startup
  unless BOTH `--validator-bin` and `--chain-config` are set (§2: a
  signer-only prover would attest composer-supplied state without
  re-executing it — unacceptable in the untrusted-composer topology).
- Conflicts: `EEZ_PROOF_SINK_URL` set alongside listen mode ⇒ startup bail
  (the sink shares the reverse channel by construction). A non-empty
  `EEZ_L2_RPC_URL` in reverse mode ⇒ startup WARN (it points at the
  composer's archive, unreachable here; see Backfill).
- **IPv4-mapped IPv6:** binding a dual-stack `[::]` listener yields peers
  as `::ffff:a.b.c.d`, which would fail a naive `IpAddr` comparison against
  `a.b.c.d`. Normalize peers with `to_canonical()` before the allowlist
  check, so operators can write plain IPv4 entries regardless of bind
  choice.

**`ComposerConn` abstraction.** All three client call sites
(`ControlFeedClient` subscribe, `dispatch_one`, `submit_slot_proof`)
change from URL-connect to `conn.channel().await`:

```rust
enum ComposerConn {
    Dial(String),        // today's mode: Endpoint::from_shared(url).connect()
    Reverse(ReverseHub), // channel over the composer's accepted dial-in
}
```

tonic `Channel` is `Clone` and multiplexes h2 streams, so feed + dispatch +
sink share one connection in reverse mode. In dial mode, semantics match
today's `XClient::connect(url)` (which is the same eager
`Endpoint::connect` internally); the `proof_sink_url`-defaults-to-
`control_addr` behavior is preserved for dial mode.

**`ReverseHub`.** A `tokio::watch<Option<Channel>>` fed by an accept-loop
task:

1. `accept()` → allowlist check (canonicalized) → `set_nodelay`.
2. Build the channel over the accepted socket:
   `Endpoint::connect_with_connector` with a **one-shot** tower service
   that yields the socket (wrapped in `hyper_util::rt::TokioIo` — tonic
   0.14 speaks hyper's rt IO traits) exactly once; any later internal
   reconnect attempt gets `NotConnected` and fails the channel, so
   replacement happens only through a fresh composer dial-in.
3. Endpoint keepalive: `http2_keep_alive_interval(30 s)`,
   `keep_alive_timeout(10 s)`, `keep_alive_while_idle(true)` — detects a
   silently-dead composer.
4. `watch.send_replace(Some(channel))` — **last-wins** (decided, §7.1): a
   new allowlisted dial-in replaces the current channel unconditionally; the
   old channel's streams die naturally. Rationale: the composer redials
   after silent deaths it detected before we did, and in the target
   deployment each composer has a **distinct** allowlisted IP, so there is
   no competing peer to preempt it. Every replacement logs the peer.
   - **Known limitation (accepted).** Last-wins is only safe when
     allowlisted peers have distinct, stable IPs. If two peers share an
     allowlisted IP (CGNAT co-tenants, §2), either can periodically preempt
     the hub channel and starve the honest composer's settlement (all
     prover→composer RPCs share this channel). Deployments with shared
     source IPs must front the listener with a per-peer tunnel (§2) or adopt
     refuse-while-healthy later. Not a concern for the distinct-IP topology
     Level 1 targets.

`ReverseHub::channel()`: return the current channel, or await the first
dial-in (logged: "waiting for the composer to dial in"). **Stale-channel
policy:** after the composer dies, the hub intentionally keeps the dead
channel; callers fail fast on it and the existing `consecutive_failures`
backoff in the main loop bounds the retry spin until the composer's redial
replaces it. No clear-on-death machinery — fail-fast + replacement is
simpler and sufficient.

**Dispatch statelessness on a shared channel.** Today `dispatch_one` opens
a fresh CONNECTION per directive; the composer's `dispatch_loop` exits via
`out_tx.closed()` when the client side drops. On a shared channel this
degrades to a fresh h2 STREAM per directive. The composer's dispatch
handler is per-RPC (tonic server handlers are stream-scoped), so the
"drop the stream ⇒ composer re-reads the next window to dispatch on the
next call" contract is preserved. **Verified for this review:**
`crates/eez-composer/src/prover_dispatch.rs` (`ProverDispatchSvc`) holds
only the shared `PostedWindows` ledger and calls `next_to_dispatch()` per
RPC — no per-connection state, so a shared channel is safe. (The current
code path calls `next_to_dispatch`, not `next_unverified` — earlier drafts
of this doc misnamed it.)

**Backfill.** `EEZ_L2_RPC_URL` points at the composer's L2 archive — by
definition unreachable in the reverse topology, so deployment sets it
explicitly **empty** (⇒ `None`, the existing fail-loud no-backfill mode).
Note the trap: merely *omitting* the var keeps its default
`http://127.0.0.1:18688`, and backfill would then hang on a gap — hence the
startup WARN above when a non-empty URL is configured in reverse mode.

The composer's replay ring does NOT guarantee 36 h. `max_events` is floored
at 131072 (~36 h at 1 blk/s of **empty** blocks) but the ring is also
hard-capped at `RING_MAX_BYTES` = 1 GiB (`control_feed.rs`), so busy blocks
with multi-MB witnesses shrink the real horizon to hours or minutes
(1 GiB / 8 MB ≈ 130 events ≈ 2 min). Consequence with backfill disabled: an
outage exceeding the ring's *actual* horizon puts the driven prover in a
**permanent fail-loud retry loop** — DATA_LOSS, window dropped, the same
directive re-dispatched every ≤30 s, cursor never advanced, and (once the
settling event is evicted) the dispatched window can never attest. Recovery
needs operator intervention: re-serve the ring history or a temporary
tunnel to the composer's L2 archive. Out of scope for Level 1; subsumed by
Level 3's prover-owned view. This is an accepted Level-1 limitation, not a
regression (dial mode has backfill precisely to avoid it).

**Unchanged:** driven checkpoints (`driven-checkpoint.json`), the window
state machine, storm guard, signing, vkey derivation.

### 4.3 Compose files

- `docker-compose.remote-prover.override.yml` (composer side): replace the
  required `EEZ_CONTROL_RPC_ADDR` with required
  `EEZ_CONTROL_DIAL_ADDR=<prover-host:port>`; keep `EEZ_COMPOSER_DRIVEN=1`;
  document that this host needs no inbound ports.
- `docker-compose.proverd.remote.yml` (prover side): listen mode
  (`EEZ_CONTROL_LISTEN_ADDR=0.0.0.0:${EEZ_CONTROL_LISTEN_PORT:-50051}`),
  required `EEZ_ALLOWED_COMPOSER_IPS`, `EEZ_L2_RPC_URL: ""` (explicit empty,
  not omitted), signer key required, `--validator-bin` + `--chain-config`
  kept (mandatory — §2/§4.2), `--control-addr` removed from the command.

## 5. Dependencies

- `eez-proverd`: + `tonic` (workspace, for `Endpoint`/`Channel`/`Uri`),
  + `tower` (workspace, `service_fn`), + `hyper-util` (`tokio` feature,
  `TokioIo`) — all already pinned in the workspace or used by `eez-node`.
- `eez-node`: `tokio-stream` already present (listen-mode
  `TcpListenerStream`); no new deps.

## 6. Testing & rollout

1. **Unit:** `ControlIo` read/write delegation; allowlist incl. the
   IPv4-mapped-IPv6 case; one-shot connector exhaustion (second connect
   attempt fails `NotConnected`); dial-loop backoff reset.
2. **Loopback E2E on one machine** (the decisive test, run against live
   chiado): composer with `EEZ_CONTROL_DIAL_ADDR=127.0.0.1:9051`, proverd
   with `EEZ_CONTROL_LISTEN_ADDR=127.0.0.1:9051`,
   `EEZ_ALLOWED_COMPOSER_IPS=127.0.0.1`. Assert: reverse-connected log on
   both sides; subscribe replays; directives flow; a full deferred
   settlement lands (`settled=true` / `ATTESTED`). Restart permutations,
   each asserting redial + resubscribe-from-checkpoint + settlement resumes:
   (a) prover restart between settlements; (b) prover restart **mid-window**;
   (c) composer restart mid-window (checkpoint resume: fresh subscribe
   recomputes `from_block`/`driven_resume`, unchanged by reverse mode).
3. **Canonicalization permutation:** proverd with
   `EEZ_CONTROL_LISTEN_ADDR=[::]:9051` (dual-stack — requires host IPv6 and
   `net.ipv6.bindv6only=0`; note many container runtimes disable it),
   composer dials `127.0.0.1:9051`, `EEZ_ALLOWED_COMPOSER_IPS=127.0.0.1`.
   Assert the `::ffff:127.0.0.1` peer is ACCEPTED and the feed replays —
   this is the only test that exercises `to_canonical()` running *before*
   the allowlist check (the item-2 loopback uses an IPv4 listener, which
   never yields a mapped peer). A unit test cannot catch a wrong-order call
   at the accept site.
4. **Misconfig diagnosability:** wrong `EEZ_ALLOWED_COMPOSER_IPS` — assert
   the composer shows steady connect-then-close backoff logs and the prover
   shows dropped-peer WARNs (the two-sided signature from §4.1).
5. **Adversarial review pass** (multi-agent, as for the address-only
   patch) before merge; findings fixed or explicitly waived. (One pass on
   this DESIGN already run — its findings are folded into this revision.)
6. **Rollout:** feature branch → review → merge. Deploying = setting the
   env vars on both sides; rollback = unsetting them (defaults are the
   current behavior). No migration, no coordination with other operators.

## 7. Resolved decisions (operator sign-off 2026-07-03)

1. **Concurrent dial-in policy:** **last-wins**, no refuse-while-healthy.
   Safe for the distinct-IP-per-composer topology Level 1 targets; the
   shared-IP starvation caveat (§4.2) is an accepted, documented limitation
   revisitable later.
2. **Port convention:** keep **50051** (the "control" port either way).
3. **Dial address form:** **IP literals only** — no DNS for now. Simpler
   failure modes; removes the DNS-hijack surface entirely.
4. **Composer-side connect timeout:** explicit **10 s**
   `tokio::time::timeout` (folded into §4.1).

## 8. Out of scope (future levels)

- Single proverd process multiplexing N composers (per-composer sessions,
  QoS/quotas against adversarial composers, shared validator scheduling).
- Authentication beyond source-IP allowlisting (mTLS, signed hellos).
- Prover-owned L1/L2 view (`EEZ_PROVER_L1_RPC_URL`,
  `docs/stateful-prover-dual-mode-analysis.md`) and ring-independent
  backfill.
- TLS on the control channel.
