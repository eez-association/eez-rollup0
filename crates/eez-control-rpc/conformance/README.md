# Executed Rollup0 streaming RPC vectors

These are recorded protobuf request/response payloads for the
[`prove.v1` wire contract](../proto/prove.proto), with a
[standalone replay client](../examples/replay_streaming.rs). Use them to check a
streaming prover's responses against expected protocol behavior. The client
connects to a running prover configured for the fixture and selected test profile.

## Cases and profiles

[`rollup0-transcripts.json`](rollup0-transcripts.json) contains 17 cases.
[`rollup0-limits-transcripts.json`](rollup0-limits-transcripts.json) contains four
more cases requiring explicit server limits. Together they contain 339 requests,
338 correlated replies, one terminal transport error, and 25 successful proofs.

| Operation | Coverage |
| --- | --- |
| Begin | Correct anchor; unsupported rollup; missing/31/33-byte hashes and roots; number overflow; invalid initial binding; repeated Begin. |
| Block | Execution; duplicate at/before tip; missing data/witness; hash widths; zero/max/gapped numbers; contradictory parent/hash/RLP; invalid witness then valid retry; cache-parent binding. |
| Finalize | Full range, one block, terminal before tip, optional subrange/last block; empty prefix; invalid range/terminal; malformed/missing ABI; mismatched parent/terminal/rollup/proof system/DA; unsupported blob/context/profile inputs; valid retry. |
| Resume | Progress after half-close and Proof; live takeover; missing/malformed/retired session; nonzero initial epoch; stale transport cannot use the new epoch. |
| Rewind | Tip, intermediate block and anchor; malformed/unknown/pruned ancestors; stale epoch; removed suffix cannot finalize; promoted anchor survives Resume. |
| Cancel | Acknowledgement and EOF; retired session cannot resume; cross-stream cancellation denied; another Composer can finish. |
| Framing/pipelining | Unknown operation; unauthorized bindings; zero/max request IDs; interleaved Composers; queued gap; half-close drains work; terminal replies close without client half-close. |
| Limits | Exact block/payload/witness quota and excess; duplicate/per-session charges; selective quota release; 4096-byte frame accepted and 4097-byte frame rejected with RESOURCE_EXHAUSTED, then Resume. |

The `profile` field separates baseline, optional session/pipelining, and
backend-specific cases. A minimal implementation can run `baseline-and-rejections`
alone. The full suite covers Rollup0's baseline and optional capabilities;
session controls, shared caching, and concurrent sessions are not baseline
requirements.
Unsupported blobs/context are negative tests of this calldata-only profile,
not requirements for a prover configured for another settlement profile.

These vectors target Rollup0's calldata-only, timeless **conditional-proof
profile**: a proof binds the transition to its selected parent but does not
attest the parent's L1 acceptance.
The L1 contract checks applicability when consuming it. They do not test the
prover-owned L1 parent authentication described for blob-based profiles in the
[streaming specification](../SPEC.md).

## Server setup and replay

Use the six-empty-block
[`captured-devnet-window-84`](../../eez-prover-stateless/tests/fixtures/captured-devnet-window-84)
fixture. Configure a streaming server with its `chain-config.json`, rollup ID,
proof system, vkey, attester and L2 system address from `oracle.json`. The signing
key is the **public Anvil test key**
`59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d`.
Never use it outside an isolated test deployment.

For normal cases, configure quotas to admit the full fixture window and enable
the optional capabilities exercised by the selected cases. Use 30-second
request/idle timeouts. Start a fresh server for the complete suite or the
cold-cache witness case. From the repository root:

```sh
# All 17 normal cases against a separately configured server:
cargo run --locked -p eez-control-rpc --example replay_streaming -- http://127.0.0.1:50061
# Minimal sequential profile:
cargo run --locked -p eez-control-rpc --example replay_streaming -- http://127.0.0.1:50061 baseline-and-rejections
# Start a separate server with --max-request-blocks 2, then:
cargo run --locked -p eez-control-rpc --example replay_streaming -- http://127.0.0.1:50061 block-quota crates/eez-control-rpc/conformance/rollup0-limits-transcripts.json
```

Each limit case declares its required `server_args`: block quota 2, payload quota
6528 bytes, witness quota 30 items, or message limit 4096 bytes. Run each against
its own configured server; the runner does not configure or launch servers.
Stop any test servers and remove temporary chain data after use, keeping only
the logs needed to explain results.

To run all cases against isolated stateless signer processes, including each
case's required limits and a cold cache, use the integration suite. It shares
the standalone client's wire replayer and reads the same vector files:

```sh
cargo test --locked -p eez-prover-stateless --test streaming_rpc
```

## Assertions and portability

Steps open named streams, send/receive hex-encoded protobuf payloads, half-close,
assert a terminal RPC error, or require EOF. Frames exclude the gRPC
compression/length prefix. `check` labels describe the exercised boundary.

The runner substitutes server-issued IDs and epochs for recorded placeholders,
checks session isolation and epoch changes, and preserves literal unknown or
malformed bindings. It ignores only `Validated.reused` and `Rejected.message`.
Status, authoritative cursor, binding, public-input hash, ordering and EOF remain
assertions. Each proof's signature must recover the configured attester over the
expected digest; a different valid ECDSA nonce is allowed. The saved deterministic
signature is a reference, not a requirement for byte-identical ECDSA output.

The replay client treats the vector files as read-only expectations; it does not
regenerate them from server responses. When updating vectors, derive expected
progress from accepted requests and verify status codes, block identities, roots
and proof digests independently of the responses being recorded.

## Fixture provenance and coverage limits

Full-fixture proof expectations come from the independent captured digest and
test signature in `oracle.json`. Derived subrange/alternate-block proofs use the
request-side protocol hash helper and signer recovery; they are not additional
on-chain proof oracles.

`invalid-witness-retry` requires a cold stateless cache: a server retaining that
execution can legitimately ignore a replacement witness. `cache-parent-binding`
also depends on conditional stateless anchor admission, not a requirement that
a stateful prover admit an incorrect anchor root.

The fixture supplies witnesses, not a Reth database; a stateful server needs its
anchor/history seeded separately. This suite does not cover nonempty Sync
effects, actionable candidate-eviction details, L1 recovery, blob/RollupX positive
conformance, or stress/deadline guarantees. Timing-dependent in-flight cancellation and
backpressure belong in runtime tests, not schedule-dependent wire vectors.
