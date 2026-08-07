# Operations

> This is an implementation guide. [`SPEC.md`](../SPEC.md) is authoritative for
> protocol behavior, configuration requirements, and compatibility.

## Startup trust roots

The operator supplies the chain document, expected rollup ID, proof-system
address, proof-system vkey, attestation key, and L2 system-transaction key.
Startup validates syntax and local relationships, including key/address
constraints, but cannot query L1 to prove that the configured proof system
registers that vkey and authorizes the derived attester. Deployment automation
must verify those external relationships.

The two private keys have different authority:

- the daemon uses the attestation key only after the complete pipeline produces
  an `AttestablePublicInputsHash`, signing it with raw/prehash ECDSA and no
  EIP-191 prefix; and
- the L2 system-transaction key reconstructs omitted legacy EIP-155 L2 system
  transactions and must derive the reserved system address.

Do not reuse them. Prefer environment injection from a secret manager over
command-line flags, which may be visible in shell history or process listings.
The checked-in [`.env.example`](../.env.example) contains placeholders only.

## Network exposure

Loopback is the safe default. The gRPC protocol does not authenticate peers, so
a non-loopback listener needs an authenticated transport, private network,
firewall, or equivalent control. Single-flight admission limits concurrent
work; it is not authentication and does not stop an authorized-but-buggy client
from holding the slot with a slow stream.

## Capacity model

The service has one global active-request slot. The following controls remain
independent:

- per-message gRPC decode size;
- aggregate known-field protobuf bytes;
- declared block count;
- aggregate witness item count;
- locally selected transaction checkpoint count;
- per-message idle timeout; and
- end-to-end request deadline.

An overlap returns `ResourceExhausted` immediately. The same status is used for
local quotas, so diagnose it from the status message and available structured
fields rather than assuming the window is semantically invalid.

## Deadlines and shutdown

The idle timeout covers each wait for the next chunk or EOF. The absolute
request deadline covers ingestion through signing. Both are checked for
monotonic-clock overflow at startup.

EVM execution and individual settlement gates are synchronous. When an RPC
times out, cancellation asks the worker to stop at the next safe boundary; it
does not interrupt the current work unit. Until the worker exits, retries are
rejected because the slot is still occupied.

Ctrl-C, and SIGTERM on Unix, stop new acceptance and begin tonic's drain. The
signal does not cancel an already admitted RPC: tonic first lets it run until
completion or its request deadline. The process then waits for any detached
worker to reach a cancellation boundary or complete and release the slot.
Supervisors should therefore allow the remaining request deadline plus the
worst-case non-interruptible cancellation tail. A forced kill trades bounded
shutdown time for abandoning validation mid-computation.

## Tracing

`RUST_LOG` controls filtering and defaults to `info`. Invalid filter syntax is
a startup error. Request spans carry a request ID, peer address, validator,
expected and wire rollup IDs, range, and block count once known.

- `info` records startup configuration without secrets and successful
  attestations.
- `warn` records expected operational or composer-controlled refusals.
- `error` records invariant failures, worker failures, and signing failures.
- `debug` adds admission, pipeline, progress, and operator-configured chain detail.
- `trace` adds per-block validation and derived execution summaries.

Use `request_id`, `phase`, `grpc_code`, and `gate` when present to correlate a
refusal. The daemon intentionally returns stable public status messages while
retaining the detailed internal cause in logs.

## Verification

The full contributor command set is maintained in
[`CONTRIBUTING.md`](../CONTRIBUTING.md#verify). A change to the pinned
[`eez-association/stateless`](https://github.com/eez-association/stateless)
fork must pass that repository's tests and strict Clippy checks before the
signer updates its commit pin; the pin update must then pass the signer checks.
