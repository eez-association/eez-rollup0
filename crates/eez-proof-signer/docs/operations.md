# Operations

> This is an implementation guide. [`SPEC.md`](../SPEC.md) is authoritative for
> protocol behavior, configuration requirements, and compatibility.

## Startup trust roots

The operator supplies the chain document, expected rollup ID, proof-system
address, proof-system vkey, attestation key, and reserved L2 system address.
Startup validates syntax and local relationships, including key/address
constraints, but cannot query L1 to prove that the configured proof system
registers that vkey and authorizes the derived attester. Deployment automation
must verify those external relationships.

The daemon uses the attestation key only after the complete pipeline produces
an `AttestablePublicInputsHash`, signing it with raw/prehash ECDSA and no
EIP-191 prefix. Omitted native L2 system transactions are reconstructed from
public chain and rollup identity without a private key.

Prefer environment injection of the attestation key from a secret manager over
command-line flags, which may be visible in shell history or process listings.
The checked-in [`.env.example`](../../eez-prover-stateless/.env.example) contains placeholders only.

## Network exposure

Loopback is the safe default. The gRPC protocol does not authenticate peers, so
a non-loopback listener needs an authenticated transport, private network,
firewall, or equivalent control. Resource quotas bound work but do not
authenticate callers.

## Capacity and deadlines

The runtime bounds active streams and retained sessions. Per-session block,
byte and witness-item quotas apply even when execution is reused. Tonic's
message limit bounds decoding independently. Stateless execution has a bounded
worker pool; stateful imports and checkpoint replay coordinate with the
follower's reconciliation lock.

Idle timeouts cover stream waits. Operation deadlines bound block execution
and Finalize, including its attestation preparation and settlement. Timed-out
execution can finish in the background while holding its backend permit or
guard. Only fully checked evidence may enter a cache; expired or superseded
operations cannot sign.

Quota failures return `ResourceExhausted`; transient transport/backend failures
use their canonical status codes. The Composer can restart a retained session
at the exact range predecessor when its history fills a quota. A window that
cannot fit a fresh session remains rejected.

Ctrl-C and SIGTERM stop listener acceptance and drain active streams. Allow
for non-interruptible execution when setting a supervisor's shutdown grace.

## Tracing

`RUST_LOG` controls filtering and defaults to `info`. Invalid filter syntax is
a startup error. Session events identify accepted ranges, validation failures
and signed hashes.

- `info` records startup configuration without secrets and successful
  attestations.
- `warn` records expected operational or composer-controlled refusals.
- `debug` adds session and operator-configured chain detail.
- `trace` adds per-block validation summaries.

Use `request_id`, `phase`, `grpc_code`, and `gate` when present to correlate a
refusal. The daemon intentionally returns stable public status messages while
retaining the detailed internal cause in logs.

## Verification

The full contributor command set is maintained in
[`CONTRIBUTING.md`](../CONTRIBUTING.md#verify). A change to the pinned
Stateless fork must pass that repository's tests and strict Clippy checks before the
signer updates its commit pin; the pin update must then pass the signer checks.
