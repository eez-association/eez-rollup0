# `eez-proof-signer` implementation specification

Status: normative anchor, successful-inbound, and successful-single-call-outbound
attestation profile, version 9.
Supersedes the zero-value-outbound profile (version 8), the
anchor-and-successful-inbound profile (version 7), anchor-only profile
(version 6), validation-only profile (version 5), and the predecessor daemon
specification (version 4): the
composer-controlled `prove.v1` wire replaces the feed transport. The
backend-output contract is designed for pluggable validation backends; the
active reference implementation currently provides only the in-process
`stateless` backend.

> **Attestation status.** Version 9 actively attests effect-free anchor batches,
> successful inbound effects, and the success-expected single-call outbound
> profile defined by §§8.5–8.8, including representable non-zero value. Failed
> inbound, lookup-bearing inbound, and richer outbound shapes still reject.
> After every settlement and DA gate passes, the daemon MUST sign and return
> the recomputed public-input hash (§9). Version 9 deliberately adopts the
> state-root-authorized successor policy in §7.4; a signature does not claim
> canonical L2 ancestry, sequencer authorization, or
> successful application to the live L1 state.

> **Transient scheduling.** `transientExecutionEntryCount` and
> `transientLookupCallCount` affect inline-versus-queued L1 dispatch but are
> deliberately absent from the deployed public-input hash. They are
> unsigned L1 scheduler inputs: the signer MUST ignore their values, and no
> signer validation or security gate may rely on the dispatch split. The L1
> contract remains responsible for validating and applying those values.

This document defines the behavior required for a compatible implementation.
Requirement statements are normative; **MUST**, **MUST NOT**, **SHOULD**, and
**MAY** have their RFC 2119 meanings. Paragraphs beginning with *Reference
note:* describe the reference implementation's choice inside an allowed
latitude; they are not normative unless they say so.

For a non-normative map of the current Rust implementation, start at
[`docs/README.md`](docs/README.md).

## 1. Responsibility

The daemon serves one gRPC method, `prove.v1.Prover/Prove`. Per call, the
composer streams one posted settlement window. The active pipeline is:

1. admit exactly the streamed window — the header's full span, ascending,
   hash-chained (section 6);
2. re-execute every block through the configured validation backend and
   obtain one associated backend output (section 7);
3. check the settling `PostBatch` against the re-derived facts (section 8);
4. sign the recomputed `publicInputsHash` (section 9); and
5. return hash and signature in the `ProveResponse`.

All five items are active for anchors, successful inbound, and the
success-expected single-call outbound subset in §8.6. Failed inbound, lookup
effects, and unsupported outbound shapes MUST be rejected during item 3,
before signing. A conforming implementation MUST reach items 4–5 after, and
only after, every applicable gate succeeds.

The daemon's protocol judgment is a stateless function of the RPC input: it
holds no chain cursor, accepted-transition cache, checkpoint, or backfill
machinery, and it persists no request-derived protocol state. A timed-out RPC
can leave non-interruptible worker activity occupying the transient admission
slot until that worker exits (§4). Once a later call is admitted, however, its
window is evaluated from scratch and no previous result affects its judgment.

It must never choose its own range, skip an unverified block, generate a zk
proof, or submit an L1 transaction. Any failed check must terminate the RPC
with a gRPC error status and produce no signature.

Terminology in this document is deliberately distinct: a *window* is the
streamed inclusive L2 block span, a *batch* is the decoded `PostBatch` calldata,
the *pipeline* is validation followed by settlement, and an *attestation* is
the final signature over the independently recomputed public-input hash.

## 2. Canonical protocol functions

| Contract | Canonical definition |
| --- | --- |
| gRPC schema | [`../eez-control-rpc/proto/prove.proto`](../eez-control-rpc/proto/prove.proto) |
| Decode `postAndVerifyBatch` | `eez_protocol::entries::decode_postbatch` — Annex A |
| Decode the active `callData` payload | Local exact, zero-copy RLP parser — §8.8 |
| Public-input hashes | `eez_protocol::public_inputs::public_inputs_hashes` — Annex C |
| Inbound decoding | `eez_protocol::entries::decode_inbound` — Annex D |
| Common cross-chain call hash | `eez_protocol::common_cross_chain_call_hash` — Annex B |
| Cross-chain Sync-block reconstruction | `eez_protocol::system_tx::build_cross_chain_sync_pairs` plus `eez_protocol::system_tx::interleave_sync_block_txs` — §8.8 |
| ECDSA signing | `eez_protocol::signer::EcdsaProofSigner` (§9, self-contained) |
| Effect-candidate positions | `eez_protocol::settlement::pair_end_positions`; §8.2 defines system classification from fork-aware recovered-signer evidence and the pinned EEZL2 address |
| Batch type | `eez_protocol::EvmBatch` (the decoded `postAndVerifyBatch` argument, Annex A) |
| Rollup id | `eez_protocol::RollupId` (encoded as Solidity `uint64` in call-hash preimages) |

A Rust implementation should reuse these definitions directly; for Rust, the
paths above are normative. For any other language, Annexes A–D are the
byte-level contract. The deployed `EEZ._verifyProofSystemBatch` computation is
the final oracle for Annex C, and section 11 supplies its concrete fixture
vector. The target protocol source is pinned by the `sync-rollups-protocol`
submodule at commit `f6226f569e9b4534d42eecf5d2e3dd6c649bc6aa`. Annex B
is aligned with that revision. Until the atomic ABI cutover is complete,
Annexes A, C, and D describe the currently implemented Rust wire profile; the
Rust paths in the table above remain normative for that profile.

## 3. Configuration

Every active option is a CLI flag with an environment-variable fallback. A
CLI flag overrides its environment variable. An environment variable set to
the empty string counts as set.

| CLI | Environment | Default |
| --- | --- | --- |
| `--listen-addr` | `EEZ_PROOF_SIGNER_ADDR` | `127.0.0.1:50061` |
| `--rollup-id` | `EEZ_ROLLUP_ID` | unset (required) |
| `--chain-config` | `EEZ_CHAIN_CONFIG` | unset (required) |
| `--vkey` | `EEZ_VKEY` | unset (required) |
| `--proof-system` | `EEZ_PROOF_SYSTEM` | unset (required) |
| `--signer-key` | `EEZ_PROOF_SIGNER_KEY` | unset (required) |
| `--attester-address` | `EEZ_ATTESTER_ADDRESS` | unset (required) |
| `--l2-system-key` | `EEZ_L2_SYSTEM_KEY` | unset (required) |
| `--max-request-blocks` | `EEZ_PROOF_SIGNER_MAX_REQUEST_BLOCKS` | `512` |
| `--max-request-bytes` | `EEZ_PROOF_SIGNER_MAX_REQUEST_BYTES` | `536870912` |
| `--max-request-witness-items` | `EEZ_PROOF_SIGNER_MAX_REQUEST_WITNESS_ITEMS` | `1000000` |
| `--max-transaction-state-checkpoints` | `EEZ_PROOF_SIGNER_MAX_TRANSACTION_STATE_CHECKPOINTS` | `8` |
| `--stream-idle-timeout-secs` | `EEZ_PROOF_SIGNER_STREAM_IDLE_TIMEOUT_SECS` | `120` |
| `--request-timeout-secs` | `EEZ_PROOF_SIGNER_REQUEST_TIMEOUT_SECS` | `600` |

The following names belong to inactive target profiles and are deliberately
not accepted by the current binary:

| Future CLI | Future environment | Intended profile |
| --- | --- | --- |
| `--validator-backend` | `EEZ_VALIDATOR_BACKEND` | Backend selection once another backend is implemented; the current binary always uses `stateless`. |
| `--validator-bin` | `EEZ_VALIDATOR_BIN` | Draft ZisK subprocess backend (Annex E). |
| `--work-dir` | `EEZ_VALIDATOR_WORKDIR` | Draft ZisK subprocess backend (Annex E). |

`rollup-id` is a required, non-zero `u64`: the rollup identity assigned by the
L1 rollup registry. It is **not** the L2 EIP-155 chain id. This value is
operator configuration and is never inferred from composer-controlled
wire data. An absent, empty, malformed, out-of-range, or zero value is
startup-fatal.

`vkey` is a required, non-zero 32-byte hexadecimal proof-system verification
key, accepted case-insensitively with an optional `0x` prefix. It is operator
configuration and is never supplied by the composer. Missing, empty,
malformed, or all-zero input is startup-fatal. It is the value registered by
the configured rollup for the configured proof system; it is **not** derived
from the signing key.
Throughout this document, `configured_vkey` denotes this exact value.

`proof-system` is the required, non-zero Ethereum address of the deployed
`ECDSAProofSystem` that will consume the signature. It is operator
configuration. The sole `B.proofSystems` address MUST equal it exactly;
accepting an arbitrary non-zero composer-selected address is insufficient.
Missing, empty, malformed, or zero input is startup-fatal.

`signer-key` is a required 32-byte secp256k1 private scalar, encoded as 64
hexadecimal characters, case-insensitively, with an optional `0x` prefix.
Missing, empty, malformed, zero, or out-of-range input is startup-fatal. Derive
its standard Ethereum address by keccak-256 hashing the 64-byte `x || y`
uncompressed public key without its SEC1 `0x04` prefix and taking the last 20
bytes. The private key MUST NOT be echoed by help output, argument errors,
`Debug`, or logs; only the derived public address may be logged. Its address
MUST differ from `eez_protocol::SYSTEM_ADDRESS`; sharing the reserved L2 identity
would collapse the separate attestation and system-transaction authorities and
is startup-fatal.

`attester-address` is the required Ethereum address configured as the deployed
proof system's authorized signer. The address derived from `signer-key` MUST
equal it exactly; a missing, malformed, or mismatched value is startup-fatal.
This public binding catches a deployment/runtime key mismatch before the daemon
accepts requests without exposing the private key.

`l2-system-key` is a second required 32-byte secp256k1 private scalar with the
same encoding and redaction rules. It MUST derive exactly
`eez_protocol::SYSTEM_ADDRESS`; any other valid key is startup-fatal. It is used
only to reproduce the legacy signed system transactions omitted from
`callData`. Reconstruction MUST use the EIP-155 chain id from the same
operator-supplied `chain-config`, the pinned EEZL2 address, gas price
`1_000_000_000`, and gas
limit `2_000_000`. This key requirement is a limitation of the current legacy
DA wire. A future wire SHOULD carry raw system transactions or adopt an
unsigned protocol system-transaction type so verifiers need no signing key.
The system key is independent from the attestation key and materially more
privileged: possession authorizes arbitrary legacy L2 transactions as
`SYSTEM_ADDRESS`, not merely proof attestations. It MUST never appear in help,
errors, `Debug`, or logs and SHOULD be provisioned by a secret manager. The
composer, followers, and signer MUST use the same system key; a mismatch
deterministically refuses raw transaction reconstruction.

The rollup id, proof-system address, vkey, and attester address are independent
deployment bindings. The operator MUST ensure
that the L1 rollup manager maps `(rollup-id, proof-system)` to the configured
`vkey` and that the deployed `ECDSAProofSystem` at `proof-system` reports the
configured `attester-address` as its authorized `signer()`. The daemon verifies
that its signing key derives that configured address, but it has no L1
oracle that can prove those relationships at startup. A mismatch therefore
produces attestations that L1 will reject; it MUST NOT be hidden by deriving or
silently replacing one configured value with another. Because §9 signs a raw
digest without chain or contract domain separation, deployments SHOULD use a
dedicated key and restrict this operator RPC to trusted network peers.
The safe default binds only to loopback. A non-loopback `listen-addr` MUST be
protected by an authenticated transport, host firewall, private network, or
equivalent access control that admits only trusted composers; this protocol
does not authenticate peers itself. Supplying either private key on a command
line can expose it through shell history or process inspection, so production
deployments SHOULD inject the environment variables from a secret manager
under a dedicated operating-system account instead.

`chain-config` is required by the active `stateless` backend and is never
supplied by the composer. It accepts either an operator-supplied bare Alloy `ChainConfig`
object or its complete `Genesis` document; the latter is preferred because it
preserves the real genesis metadata. The draft `zisk` subprocess contract
reserves the bare `.config` form defined in Annex E. Parsing is strict:
unsupported top-level `Genesis` fields and unsupported `ChainConfig` fields
are startup-fatal. This prevents misspelled fork fields from being silently
ignored and starting the daemon with different consensus rules than the
operator intended.

The request quotas are service protections, not protocol semantics (section
4). All MUST be non-zero except `max-transaction-state-checkpoints`, which MAY
be zero: zero disables every non-empty checkpoint selection while preserving
an explicitly empty settling-block selection. Both timeout durations MUST also
be representable as additions to the implementation's monotonic clock;
otherwise startup is fatal rather than risking a later deadline overflow. The
default checkpoint limit is eight. A window within every quota is judged only
by sections 5–8.

Backend-specific options (execution chain config, state directories,
subprocess paths) are defined by the backend annexes; a
backend's required options are startup-fatal when missing exactly like core
options. The active `vkey`, `proof-system`, attestation signer, and L2 system
key are mandatory; there is
no unsigned or zero-vkey mode, and all settlement gates always run.

## 4. The `prove.v1` wire

The daemon binds `listen-addr` and serves `prove.v1.Prover`. The composer is
the gRPC client: it dials the daemon and client-streams one window per
`Prove` call. One exchange carries everything — there is no dispatch, feed,
or proof-sink service.

Stream discipline (each violation refuses the RPC, and MAY refuse as soon as
the offending chunk arrives — nothing requires draining the stream first):

1. The first chunk MUST be the `ProveHeader` — exactly one per stream. An
   empty stream, a block-first stream, a duplicate header, or a chunk with no
   `kind` refuses.
2. The header MUST carry a `post_batch`, and its bounds fix the block count
   up front: a block chunk beyond the declared span refuses, and a stream
   that ends before delivering the full span refuses.
3. Every following chunk is one `BlockWitness`, validated per section 6 as it
   arrives; every block MUST carry its `witness` message.
4. After sending the last declared block, the client MUST half-close the
   request stream. The daemon finalizes the window only after observing EOF;
   keeping the stream open is still subject to the idle and request deadlines.
5. On full verification the daemon returns the `ProveResponse`; on any
   failure it returns a gRPC error status and no signature.

The daemon MUST bound what one RPC can make it hold or do, refusing on the
configured quotas of section 3: the declared block span
(`max-request-blocks`), the aggregate canonical protobuf size of decoded
known fields (`max-request-bytes`), the aggregate witness-array elements
(`max-request-witness-items`), the locally selected settling-block transaction
checkpoints (`max-transaction-state-checkpoints`), the silence between chunks
(`stream-idle-timeout-secs`), and the end-to-end ingestion, validation,
settlement, and signing deadline (`request-timeout-secs`). Unknown protobuf
fields and non-canonical protobuf encoding overhead discarded by decoding do
not count toward the aggregate known-field quota; the independent per-message
decode ceiling bounds each encoded protobuf message body. The daemon MUST
admit at most one active request across all connections; an overlapping
request is refused rather than queued. Resource refusals are service
decisions, not protocol judgments — the same window with available capacity
and under laxer quotas is judged only by sections 5–8.

The checkpoint quota bounds the number of locally selected positions and
therefore the number of checkpoint trie reconstructions; it does not bound
their individual cost. It does not by itself bound witness size or the product
of checkpoint count and witness size; the byte and witness-item quotas remain
independent and necessary. Exceeding the checkpoint quota maps to
`resource_exhausted`, including when the configured limit is zero.

*Reference note:* the implementation acquires the single request guard before
reading the first chunk and retains it through validation, settlement,
attestation, and response construction. The guard moves into the blocking task
while that task runs. If the RPC deadline expires after the task starts, the
detached worker retains the slot until it exits. Cancellation is cooperative:
Stateless polls before final-block preparation and before each block execution;
settlement polls before decoding and again before the DA gate. Each block
execution and each settlement gate is synchronous and non-interruptible. Holding
admission during ingestion also means a slow but non-idle stream consumes the
slot. Deployments therefore SHOULD expose this operator endpoint only to
authenticated or otherwise network-trusted composers.

A graceful shutdown MUST NOT complete while admitted request work is still
running. *Reference note:* Ctrl-C, and SIGTERM on Unix, stop new
connections, let tonic drain request futures, and then wait for the request
guard to become idle. The implementation does not force-kill an already-running
blocking task, so shutdown can wait beyond the task's original RPC deadline.

Implementations MUST configure an explicit per-message gRPC decode limit for
large block chunks (consensus RLP plus full execution witness); tonic's 4 MiB
default is insufficient. The reference ceiling for one message is the smaller
of `max-request-bytes` and 256 MiB. Consequently, satisfying the aggregate
request quota does not imply that an individual message above this independent
ceiling is accepted.

*Reference note:* the reference caps the per-message decode limit at 256 MiB,
uses a 1 KiB response encoding ceiling, and maps refusals to
`Status::invalid_argument` (malformed stream or window fields, window-shape
admission, malformed settlement calldata, or a malformed/non-exact nested
`callData` payload), `resource_exhausted` (quota or an overlapping request),
`deadline_exceeded` (idle and request timeouts), `cancelled` (request/transport
cancellation), `failed_precondition` (a
configured-rollup identity mismatch, backend rejection, or semantic-gate
rejection such as payload/window disagreement), and `internal` (an invalid
successful backend output, worker failure, an internal invariant, or a signing
error). A window that passes every active profile gate and completes
attestation within the deadline returns a successful `ProveResponse`.

One per-request blocking worker runs CPU-heavy validation and the complete
settlement gate sequence; signing runs after that worker returns. This keeps
the asynchronous server responsive while the deadline remains enforceable at
the boundaries around non-interruptible work.

## 5. Wire-field authority

`ProveHeader` defines the inclusive window `[from_block, to_block]`,
`from_block >= 1` and `from_block <= to_block`; anything else refuses the
RPC. `ProveHeader.rollup_id` MUST equal the configured `rollup-id`.
The daemon MUST enforce this identity check while admitting the header,
before backend validation; a mismatch refuses the RPC without waiting for or
validating block chunks. The header remains a composer claim rather than an
authority: equality with operator configuration is what makes it admissible.
Section 8.3 independently binds the decoded `PostBatch` to the same
operator-configured identity.

Within the header's `PostBatch`:

- `abi_calldata` is the sole submitted batch payload consumed by the daemon;
  it remains untrusted until every applicable request-pipeline gate succeeds;
- `public_inputs_hash` is non-authoritative Composer wire data and MUST be
  ignored; it still counts toward the request byte quota but MAY be discarded
  immediately afterward, and only the independently recomputed hash may be
  signed (§8.1); and
- `l1_block_hash` MUST have length 0 (§8.1 pins the timeless batch shape).

Each `BlockWitness` supplies `number` (a protobuf `uint64`), `hash` and
`parent_hash` as exactly 32 bytes each (any other length refuses the RPC),
consensus block RLP in `rlp`, and a `witness` message with the four arrays
`state`, `codes`, `keys`, `headers` — a block without a `witness` message
refuses the RPC. The block at `to_block` is the settling block.

Wire values are composer-claimed until re-derived: block hashes bind through
re-execution (§7), and the batch binds through the gates (§8).

## 6. Window admission

Admit the streamed blocks against the header before any validation
(incrementally per chunk or after the stream ends — the accepted set is
identical):

1. the block count MUST equal `to_block - from_block + 1` — both a surplus
   block and a stream that ends early refuse;
2. block `i` (zero-based) MUST have `number == from_block + i` — a gap,
   duplicate, or reordering refuses;
3. each block's `parent_hash` MUST equal the previous block's `hash`. The
   first block's parent hash is length-checked only: with nothing earlier to
   chain against, re-execution binds it only to the parent header supplied in
   the witness; it does not independently establish canonicality (§7.4).

Any violation refuses the whole RPC. Never admit a partial prefix, and never
skip past a block that was not streamed: a state-root equality does not prove
block-number or block-hash continuity.

## 7. Validation

Validation uses a backend-neutral `BackendWindowOutput` contract. Every backend
re-derives what the admitted window did by re-executing it. Each
`BackendBlockOutput` keeps one block's computed commitments, receipt outcomes,
checkpoints, and settlement evidence together. The shared validation layer
consumes that output with the exact admitted block data, contract-checks every
association, and normalizes the result into a settlement-ready
`ValidatedWindow`. Settlement does not consume unchecked parallel vectors and
does not need to know which backend produced the output. The current reference
implementation always uses `stateless`; backend selection remains a target
capability.

### 7.1 Backend output

One `BackendWindowOutput` covers the whole admitted window. A backend MAY
validate in internal sub-chunks, but then MUST verify that consecutive
sub-chunk results telescope, with each sub-chunk pre-state root equal to the
previous sub-chunk's final post-state root, and MUST merge them into one output;
a telescope mismatch rejects the window. Every admitted block RLP MUST decode
exactly, consuming the complete input; malformed RLP or trailing bytes reject
during backend validation. Successful output therefore guarantees that
settlement gates can decode the same blocks.

| Field | Rule |
| --- | --- |
| `pre_state_root` | Required. Validated state root from which the window's first block was executed. Normalization retains it as `window_pre_state_root` for §8.3. |
| `blocks` | Required. One entry per admitted block, oldest first; length MUST equal the window length. |
| `blocks[i].decoded_number` | Required. Number exact-decoded from the block RLP; MUST equal `BlockWitness.number`. |
| `blocks[i].decoded_parent_hash` | Required. Parent hash exact-decoded from the block RLP; MUST equal `BlockWitness.parent_hash`. |
| `blocks[i].computed_hash` | Required. The re-derived block hash; MUST equal the streamed `BlockWitness.hash`. |
| `blocks[i].decoded_transaction_count` | Required. Transaction count in the exact-decoded block body; MUST equal the length of `receipt_successes` and `system_sender_flags`. |
| `blocks[i].receipt_successes` | Required. Element `j` is transaction `j`'s receipt-success boolean, in block order. Its length MUST equal the decoded transaction count for block `i`. |
| `blocks[i].post_state_root` | Required. State root recomputed after block `i` and matched to that block's header commitment. The final element is normalized as `window_post_state_root`. |
| `blocks[i].transaction_state_checkpoints` | Required vector. Each element is `(transaction_index, state_root)`, where `state_root` is the verified cumulative state root after the zero-based `transaction_index` of block `i`, including pre-block changes and transactions `0..=transaction_index`, but before post-block state changes. Every preceding block's vector MUST be empty. The settling block's vector MUST contain the locally selected positions in strictly increasing order and MAY be empty. Every index MUST be smaller than that block's decoded transaction count. A checkpoint after the final transaction need not equal the block's post-block state root because withdrawals or other post-block processing can change the root. |
| `blocks[i].settlement_evidence.system_sender_flags` | Required. Element `j` records whether transaction `j`'s signer, recovered under the configured chain rules, is `SYSTEM_ADDRESS`. Its length MUST equal the decoded transaction count for block `i`. |
| `blocks[i].settlement_evidence.observed_outbound_events` | Required ordered vector containing every outbound-event candidate derived from block `i`'s verified receipts, with the coordinates and strict decoding behavior below. |

The daemon MUST verify the block count, every decoded identity and hash
association, decoded transaction count, receipt and
system-sender coverage, outbound-observation transaction-index bounds and
coordinate order, and checkpoint
shape after every backend call; a backend is not assumed to self-enforce its
output contract. A violation is invalid successful backend output and maps to
`internal`; a backend that rejects the input before claiming success maps to
`failed_precondition`.

A backend MUST include a checkpoint only when it can prove that checkpoint
from the same execution whose complete post-block state was validated. It MUST
NOT synthesize a checkpoint or substitute the block's post-state root for an
unavailable transaction checkpoint. An empty vector authorizes no transaction
position. A sparse non-empty settling vector proves only its indexed positions;
a consumer MUST NOT infer checkpoints for unreported indices.

The system-sender flags MUST come from the same validated execution as their
associated block output. An external backend would need an authenticated
transport defined by its own contract. They are locally derived settlement
evidence, not Composer input. A missing or mis-sized vector after successful
validation is an invalid backend result and maps to `internal`.

The same execution MUST provide every outbound-event candidate from every
block's verified receipts. A candidate is a log emitted by the pinned EEZL2
address whose `topic0` is the ABI-derived `CrossChainCallExecuted` signature.
Retain its zero-based transaction index and receipt-local log index. When the
complete topics and ABI body decode and re-encode exactly, also retain the call
hash and `uint64 callGas`; otherwise retain the named candidate without decoded
event fields rather than dropping it. Preserve receipt/log order and duplicates.
These observations are associated settlement evidence, not Composer input.

### 7.2 Evidence scope

Every backend-output field in §7.1 is mandatory. An empty settling checkpoint
vector authorizes no effect candidate. A sparse vector MAY contain only locally
selected indices, but §8.4 requires exact coverage of the positions used by
the effect gate: missing, extra, duplicate, or reordered checkpoints reject.
Transaction checkpoints provide positional state-root evidence, but do not by
themselves prove effect identity, return data, or execution outcome. Outbound
authorization therefore additionally requires the exact event, entry-shape,
accounting, and DA bindings in §§8.6–8.8. A backend MUST NOT report success for
any block it did not verify.

Outbound-event observations, whether absent, valid, or malformed, do not by
themselves authorize an outbound effect. They are one input to the complete
authorization proof in §§8.6–8.8.

### 7.3 Backend profiles

| Backend | Availability | Method | Output envelope |
| --- | --- | --- | --- |
| `stateless` | Active; always selected by the current binary. | In-process re-execution using the checkpoint-capable [`eez-association/stateless`](https://github.com/eez-association/stateless) fork pinned to an exact commit. | MUST emit every required field with each block's computed hash, post-state root, receipt successes, checkpoints, and settlement evidence associated in one `BackendBlockOutput`. Preceding blocks MUST emit empty `transaction_state_checkpoints` vectors. The settling block MUST emit `c`, where `c` exactly matches the locally selected ordered positions; `c` MAY be empty. |
| `stateful` | Future; not implemented or selectable. | In-process re-execution against locally maintained chain state, without witnesses. | MUST emit every field. Its state bootstrap and persistence rules are pinned by its annex when it lands. |
| `zisk` | Draft; not implemented or selectable. | The `native-validate` subprocess — Annex E. | MUST populate the complete §7.1 output before it can be selected. |

Populating §7.1 is necessary but not sufficient for a backend used with the
active settlement profile. Annex E does not yet transport the required
`settlement_evidence`, and therefore remains unselectable.

The pinned `eez-association/stateless` fork is derived from upstream commit
`3d2fc174df31f5b0d5d4d831dc7e1607ea541531` and adds selected transaction-state
checkpoints without replacing upstream consensus or execution validation. The
exact fork revision is recorded in this crate's Cargo manifest. Changing either
the upstream base or the pinned fork revision is an explicit validator upgrade
and requires the section-7 conformance suite.

For a batch that a re-executing backend can fully validate, the backends carry
equal validation strength: the daemon executed the window itself. The active
adapter derives the settling-block selection locally from the exact decoded
block and the senders recovered under the configured `ChainSpec`, applies the
operator quota, and never accepts positions supplied by the Composer. If the
selection is empty, it uses the checkpoint-free validation path and returns
`[]`. A non-empty selection within quota uses the checkpoint API on the same recovered
block and witness; the returned indices MUST exactly match the requested
selection. A selection above quota is instead a resource refusal. This allows
§8.4 to validate non-empty effect prefixes. The active profile admits the
successful inbound subset defined by §8.5 and the success-expected single-call
outbound subset defined by §8.6.

The active adapter associates the fork-aware `SYSTEM_ADDRESS` signer flag with
every transaction and uses the settling block's evidence both for checkpoint
selection and settlement classification. It also uses the same evidence to
forbid privileged transactions in preceding blocks. Settlement MUST NOT
independently recover those signers: checked low-`s` recovery can disagree with
valid pre-Homestead recovery. The stateless backend is the baseline every
deployment can run, and a conforming implementation MUST provide it.

The adapter also extracts the ordered outbound-event candidates from the same
receipts that produced `receipt_successes`. Candidates from preceding blocks
reject; the settling block's candidates are retained for §8.6. No EEZ-specific
event interpretation belongs in the generic Stateless library.

### 7.4 State-root authority and successor selection

Version 9 retains version 8's deliberate selection of transition validity
rather than independent fork choice. The Stateless backend proves that each
supplied block is valid under the configured chain rules and that the supplied
sequence telescopes from `BackendWindowOutput.pre_state_root` to the final
`BackendBlockOutput.post_state_root`. Its first parent header still comes from
the Composer-supplied witness: an
attestation does not certify that the parent block is canonical at the supplied
height, that a sequencer authorized it, or that the sequence belongs to an
independently selected L2 chain.

Eligibility is instead state-root based. A transition is eligible when it
passes §§6–8 from the normalized `window_pre_state_root`. Section 8.3 requires
the leading state delta's `currentState` to equal that root, and §8.1 commits the
complete entry, including `currentState` and `newState`, in the recomputed
public-input hash. The live L1 rollup state is the application authority: when
an entry is consumed, `_applyStateDeltas` requires the live state root to equal
its `currentState` before applying `newState`.

Competing valid transitions from the same root may therefore both be attested.
Whichever matching entry is successfully consumed first can mutate the L1
root; a sibling made stale by that mutation cannot. This is an intentional
**first-applicable-valid-transition** policy, not signer-side fork choice.
Proof verification or batch posting is not itself the selector: an immediate
entry whose root is stale reverts inside `attemptApplyImmediate`, is caught and
skipped by `postAndVerifyBatch`, and does not necessarily revert the batch. A
deferred entry is checked only when later consumed; a failed consumption
reverts its queue-cursor update as well, so that entry remains pending. Because
the transient counts are not public-input fields, an attestation certifies
neither dispatch mode nor successful application.

This policy accepts the associated liveness and ordering tradeoffs. Stale or
unconsumed attestations can still be posted or queued, and a previously seen
transition can become applicable again if the rollup later returns to the same
state root. A future profile that needs canonical block ancestry, monotonic
height, or sequencer authority MUST add and commit an independent authority
mechanism; hash continuity within one request is not such a mechanism.

### 7.5 Execution checkpoints and the future blob stream

Indexed transaction state checkpoints are backend execution facts, not
protocol-level "pair roots" or blob messages. A checkpoint's
`transaction_index` is always a zero-based position in the decoded block; its
`state_root` always denotes the cumulative execution state immediately after
that transaction and before post-block processing. The current EVM profile
selects particular positions from the settling block when binding settlement
effects, but that selection MUST NOT change either meaning.

The draft [standardized blob message
format](https://github.com/eez-association/eez-core-protocol/pull/29) replaces
the current system/user-pair transport with a semantic message stream:
`ChainOperation`, `InitiateCrossChainTransaction`, `Call` / `StaticCall`,
`ReturnSuccess` / `ReturnFail`, `Snapshot` / `Revert`, and
`FinishCrossChainTransaction`. That draft does not yet define state roots or
a mapping from messages to chain-specific execution checkpoints. A future
blob profile MUST define that mapping, including rollback semantics, before
using message positions as state-root provenance. It MUST NOT align messages
with any block's `transaction_state_checkpoints` by ordinal, interpret a
checkpoint index as a message index, or rename transaction checkpoints as
message roots. The blob parser's semantic observations and a chain backend's
execution checkpoints are separate evidence joined only by an explicitly
specified position mapping.

Receipt coordinates are separate again: an outbound observation's
`transaction_index` and receipt-local `log_index` locate an event-shaped log in
the re-executed receipts. Only a complete canonical decode turns that candidate
into usable call-hash evidence. Neither coordinate is a blob-message position,
and neither creates the missing mapping between the future message stream and
execution checkpoints.

## 8. Settlement gates

These gates run after `BackendWindowOutput` has been cross-checked against the
admitted blocks and normalized into the `ValidatedWindow` described in §7.
Decode `PostBatch.abi_calldata` once as batch `B` (Annex A); decode failure
refuses the RPC. Throughout this section, `window_pre_state_root`,
`settling_pre_state_root`, and `window_post_state_root` are the normalized
execution roots. `receipt_successes`, `transaction_state_checkpoints`, and
`settlement_evidence` remain associated with `settling_block`.

### 8.1 Public-input hash

For every batch, require `PostBatch.l1_block_hash` to have length 0, then:

1. require the pure structural rules in Annex C, including exactly one
   proof-system address, and require it to equal the configured
   `proof-system` exactly;
2. require `B.rollupIdsWithProofSystems` to contain exactly one row,
   whose `rollupId` equals the configured `rollup-id` and whose
   complete `proofSystemIndex` array is exactly `[0]`, and require every entry
   and lookup rollup reference covered by Annex C to name that sole row;
3. require `B.crossProofSystemInteractions == bytes32(0)`; the active
   single-proof-system profile has no cross-PS boundary to derive or verify;
4. require `B.blockNumber == 0`;
5. require `B.blobIndices` to be empty — this daemon has no independent
   transaction-blob oracle from which to resolve `blobhash(index)`;
6. compute `public_inputs_hashes(B, configured_vkey, absent)` exactly as
   Annex C;
7. require exactly one result.

The single-assignment, one-proof-system, zero-cross-PS, and timeless rules are
deliberate fail-closed pins. Annex C substitutes the same independently
configured vkey into the sole rollup row assigned to proof-system index `0`.
The proof-system address selects the verifier but is not itself part of the
deployed public-input hash; equality with operator configuration is therefore a
mandatory pre-signing gate, not a cosmetic shape check. Allowing another
rollup row would authorize this signer for a composer-selected rollup even
though the header and state-delta chain name the configured rollup. A multi-PS
batch would leave `PS[1..]` unconstrained by the single wire hash, and there is
no circuit in this profile from which to derive a non-zero cross-PS commitment.
A bound (`blockNumber != 0`) batch would fold a composer-supplied L1 blockhash
that this daemon has no independent L1 oracle to verify. Widening any of these
pins requires a spec revision.

The signature input is this recomputed hash. Never read or sign the header's
`public_inputs_hash`; return only the recomputed hash.

### 8.2 System transactions and legacy effect candidates

Decode the settling block RLP exactly, consuming the complete input. Malformed
RLP or trailing bytes reject the window. This is a defense-in-depth check:
section 7 already requires every successful backend to have decoded and
re-executed the same exact block.

Transaction `i` is a system transaction exactly when both hold (a create
transaction — no `to` — means not-system):

```text
recovered signer == 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
to               == 0x4200000000000000000000000000000000000007
```

The active adapter obtains `recovered signer` from recovery of each exact
decoded block under the operator-configured `ChainSpec`; no composer metadata or
settlement entry participates. Successful validation retains whether that
signer equals `SYSTEM_ADDRESS` for every transaction. An unavailable signer
rejects validation, so downstream code MUST NOT silently reinterpret recovery
failure as not-system. Settlement exact-decodes the retained RLP again, binds
the retained vector to its transaction count and order, and combines each flag
with the transaction's exact recipient and calldata. It never performs a
second signature recovery.

Every settling-block transaction whose recovered signer is `SYSTEM_ADDRESS`
MUST target the canonical EEZL2 address; a create or any other recipient
rejects. This is stricter than the two-part classification above and prevents
the holder of the protocol key from directly occupying an ordinary-user
position. Later DA checks further require every admitted EEZL2 system
transaction to be one of the exact canonical transactions reconstructed for
the effect plan. This role-separation rule is defense in depth; it does not
make the keyed system account an economic sink (§8.6).

Every block before the settling block MUST contain neither an EIP-2718
type-`0x7E` transaction nor a transaction whose recovered signer is
`SYSTEM_ADDRESS`, regardless of its recipient or calldata. Decode those block
RLPs exactly when enforcing this rule and bind their transaction counts to the
retained fork-aware signer facts from §7. A transaction type unsupported by
the active decoder, including type `0x7E` in the currently pinned Ethereum
primitives, rejects during exact decoding and therefore satisfies this rule
fail-closed. This framing-wide rule is intentionally stricter than the
settling-block classification above. An intermediate system transaction could
load an execution table and have a later transaction in the same block consume
it, or otherwise create a cross-chain effect for which the settling-block
backend output has no positional evidence. The canonical Composer therefore
reserves every pre-settling block for user-originated transactions.

In the current EVM block framing, position `i` is an effect candidate when:

```text
!system[i] || i is last || system[i + 1]
```

This formula recognizes transport boundaries only; it does not prove that a
cross-chain effect occurred. In particular, an ordinary or reverted user
transaction is still a candidate. Sections 8.5–8.6 must derive the semantic
effect from the exact transaction and its receipt evidence before the active
effect-bearing profile admits it. A future blob-stream profile replaces this
formula with semantic message observations under §7.5.

The settling block's `receipt_successes` MUST have exactly one element per
decoded transaction, with element `i` being transaction `i`'s receipt status,
and every system transaction MUST have status `true`. User reverts are allowed.
A missing or mis-sized vector is invalid successful backend output and maps to
`internal`; a verified system transaction whose status is `false` is a policy
rejection and maps to `failed_precondition`. Section 8.5 additionally requires
a successful status for each admitted inbound transaction.

### 8.3 State-delta chain

Let `entries = B.entries`. Require it to be non-empty and require
exactly one `StateDelta D[i]` per entry. Define `settled_rollup` by a checked
conversion of `D[0].rollupId` to `u64`; refuse if it does not fit or is zero.
Require `settled_rollup == configured rollup-id` before using it to classify
or authorize any batch effect.

```text
D[0].currentState  == window_pre_state_root
D[last].newState   == window_post_state_root
D[i].newState      == D[i + 1].currentState
D[i].rollupId      == uint256(settled_rollup)
```

The header check in §5 and this batch check are distinct trust-boundary gates:
the header gate rejects a mismatched stream identity before validation, while this
gate runs only after backend validation and exact batch decoding. Passing one
MUST NOT skip the other. Together they require both composer claims — the
window header and the `PostBatch` state-delta chain — to name the same
operator-configured rollup.

This gate validates topology only. Merely observing an interior value somewhere
in the backend output is not proof that the claimed transition occurred at that
point. Section 8.4 therefore binds every interior boundary to its exact
re-executed provenance and position.

### 8.4 Exact effect prefixes

Classify batch entries in order:

```text
Inbound   proxyEntryHash != 0
Outbound  proxyEntryHash == 0 and l2ToL1Calls is non-empty
Anchor    proxyEntryHash == 0
          and destinationRollupId == uint256(settled_rollup)
          and l2ToL1Calls, expectedL1ToL2Calls, and expectedLookups are empty
          and callCount == 0
          and returnData is empty
          and rollingHash == 0
Invalid   otherwise
```

The claimed sequence MUST contain exactly one leading `Anchor`, no later
anchor, and no `Invalid` entry. Let `effects = entries[1..]`. Section 8.7
separately and unconditionally requires the anchor's sole state delta to have
`etherDelta == 0`. The exact anchor shape is load-bearing: an immediate entry
with inconsistent execution fields can revert inside
`EEZ.attemptApplyImmediate`; `EEZ` catches that revert and skips the entry,
including its state delta, instead of reverting the batch.

If `effects` is empty, require the anchor's
`newState == window_post_state_root`. Otherwise require the anchor's
`newState == settling_pre_state_root`, the state immediately before the
settling block:

- for a one-block window, `settling_pre_state_root` is
  `window_pre_state_root`; and
- for a multi-block window, `settling_pre_state_root` is the
  `post_state_root` associated with the last preceding `BackendBlockOutput`.

For the current legacy prefix gate, provisionally classify each effect
candidate position `p[j]` (§8.2, in order). Every candidate participates in the
count check, including a plain user transaction, which is provisionally
classified as `Outbound`:

```text
kind[j] = Inbound if system[p[j]], otherwise Outbound
```

This classification is structural and MUST NOT be treated as effect
authorization. Let the mandatory settling-block
`transaction_state_checkpoints` vector be the ordered list `c`. Its index
sequence MUST cover the candidate positions exactly and ordinally:

```text
len(c) == len(p)
c[j].transaction_index == p[j]  for every j
```

Thus a missing checkpoint, an extra checkpoint, or a checkpoint for the right
position in the wrong ordinal rejects. The general §7.1 rules separately
reject duplicate, unordered, and out-of-range indices. Claimed effects,
candidate positions, and checkpoints MUST have identical counts, and claimed
effects and candidates MUST have identical provisional kind order. Therefore
an empty `c` is valid only when `p` and `effects` are also empty; it authorizes
no transaction.

Match claimed effect `j` directly with `c[j]`; do not search by root value.
For every claimed effect, in order:

```text
entry.newState == c[j].state_root
```

Section 8.3 already requires its `currentState` to equal the immediately
preceding entry's `newState`. Positional matching is intentional: duplicate
root values remain ordinally unambiguous. Semantic identity still comes from
the later consumers of the retained effect plan.

The implementation MUST retain this validated correspondence as an ordered
effect plan `(entry_index, transaction_index, kind)`. Later semantic gates
MUST consume that plan instead of deriving candidate positions or kinds a
second time. This makes the positional proof established here the single
source of truth for subsequent inbound and outbound bindings.

The last transaction of a non-empty settling block is always an effect
candidate under §8.2. Sections 8.3 and 8.4 therefore jointly require the final
checkpoint used by an effect-bearing batch to equal `window_post_state_root`.
The current profile consequently rejects an effect-bearing block when post-block
processing changes state after its last transaction. Supporting such a block
requires a future, explicitly specified transition from the final transaction
checkpoint to the post-block root; an implementation MUST NOT relabel a
pre-post-block checkpoint as the final root.

### 8.5 Successful inbound authorization

This gate runs on **every** settling batch. Zero inbound remains valid, but
every admitted inbound effect MUST satisfy the complete binding below and
`B.l1ToL2lookupCalls` MUST be empty. Lookup-bearing and failed inbound
effects remain unsupported and MUST reject before attestation.

An *inbound candidate* is any transaction whose input begins with the Annex D
selector, whose recovered signer is `SYSTEM_ADDRESS`, and whose recipient is
the active EEZL2 address pinned in §8.2. Selector-shaped calldata from any
other sender or to any other recipient remains an ordinary transaction.
Pre-settling blocks are covered by §8.2's stronger prohibition on every
`SYSTEM_ADDRESS`-signed transaction.

The settling-block inspection MUST retain every candidate's position. It MUST
derive a strict observation only after all of these checks succeed:

- a present successful receipt status for the exact transaction;
- complete canonical decoding of the outer ABI, native transaction value
  equal to outer `value`, and `sourceRollup == 0`;
- exactly one outer `entries` element, no outer lookup calls, exactly one
  `entries[0].incomingCalls` element, and `entries[0].callCount == 1`;
- empty nested expected-lookups and outgoing-call tables;
- equality of every outer and inner call field (`destination` /
  `targetAddress`, `value`, `data`, `sourceAddress`, and `sourceRollup` /
  `sourceRollupId`) and `revertSpan == 0`; and
- a non-zero recomputation of the common mutable hash over source
  `(sourceAddress, RollupId(0))` and target `(destination, settled_rollup)`,
  with `value` and `data`, equal to the inner `proxyEntryHash`.

Only after these checks may the shared decoder recover the claimed success
flag and return bytes from the rolling hash. The recovered outcome MUST be
successful. A successful transaction receipt alone is insufficient: it proves
that the outer EEZL2 call did not revert, not that arbitrary inner fields
describe the executed call.

Consume §8.4's ordered effect plan exactly once. Every provisionally
`Inbound` item MUST match exactly one strict candidate at the same transaction
index and the entry at the planned batch index. Missing, extra, hidden,
invalid, failed, or reordered candidates reject. Equal hashes are not
deduplicated; the match is positional and duplicate-preserving.

The matched on-chain deferred entry MUST have exactly one state delta, target
the configured rollup, and use the lean shape with empty `l2ToL1Calls`,
`expectedL1ToL2Calls`, and `expectedLookups`, zero `callCount`, and zero
`rollingHash`. Its `proxyEntryHash` and `returnData` MUST equal the strict
observation. Its `etherDelta` MUST equal the checked `int256(value)` from that
observation; values outside the non-negative Solidity `int256` range reject.

The implementation MUST retain the successful ordered bindings — including
each transaction position and its canonical derivation-sidecar projection — as an
opaque proof object consumed by §8.8. A caller MUST NOT construct the
positive-inbound DA profile without first passing this gate.

The profile assumes that the code at the pinned EEZL2 address has the
canonical protocol semantics. Stateless execution proves behavior under the
validated witness-backed state root, but this gate pins the address rather than
a code hash or implementation identifier. Deployments MUST preserve that
identity through their root/upgrade policy; pinning code identity explicitly
is a future hardening option.

Failed inbound additionally needs a distinct effect classification and
root-provenance rule for its lookup-plus-settlement-entry shape. It MUST NOT be
inferred by treating arbitrary later anchor-shaped entries as effects.

### 8.6 Outbound authorization (single-call success-expected profile active)

The active outbound gate consumes §8.4's exact effect plan and the positioned
receipt observations from §7.1. Every provisionally
`Outbound` effect MUST have exactly one canonical observation at the same
transaction index; missing, extra, multiple, malformed, reassociated, or
inbound-position observations reject. Its batch entry MUST contain exactly one
`l2ToL1Call`, target the configured rollup, and name that rollup as the call's
source. The supported EEZL2 deployment has `USE_GAS_LEFT` disabled, so the
observation's manager-entry `callGas` MUST be zero. Recompute the mutable L2
outbound hash as
`keccak256(abi.encode(false, sourceAddress, uint64(sourceRollupId),
targetAddress, uint64(MAINNET), value, uint64(callGas), data))`, then require
equality with the observed event hash. Matching is positional and
duplicate-preserving: equal hashes at distinct effect positions remain distinct
observations. Every intermediate-block observation rejects. The call's
`sourceAddress` MUST NOT be `SYSTEM_ADDRESS`; that identity is reserved for
protocol transactions rather than application-originated effects. The event's
`callGas` is part of this source-side hash; it is not the destination-call gas
limit carried by a cross-chain call record.

The gate then restricts every outbound entry to the success-expected
single-call subset supported by this profile. The entry schema and wider
protocol can represent richer `revertSpan` shapes, but the current Sync builder
does not preserve that field and they are deliberately excluded here. For
every such entry, both `expectedL1ToL2Calls` and `expectedLookups` MUST be
empty, `callCount` MUST be one, and the sole call's `revertSpan` MUST be zero.
Recompute the Annex D rolling hash as `CALL_BEGIN(1)` followed by
`CALL_END(1, true, entry.returnData)` and require exact equality with
`entry.rollingHash`; a failed or otherwise unrecognized outcome rejects. The
sole call's `value` MAY be zero or non-zero but MUST fit the non-negative
Solidity `int256` range. Its sole StateDelta MUST have
`etherDelta == -int256(value)` as required by §8.7.

The zero-`revertSpan` restriction is load-bearing. A non-zero span executes in
an isolated context whose `etherOut` is discarded when that context is
reverted, so flattening it into the current accounting rule would be wrong.
Neither the event hash nor the current Stateless backend output authenticates
`revertSpan`; richer span-aware evidence is outside this profile.

L2 value custody and protocol-transaction fee funding are deployment
invariants outside this attestation. The intended deployment leaves
`SYSTEM_ADDRESS` unfunded and funds value-bearing protocol execution from
EEZL2. The current legacy system-transaction form does not itself implement
that funding rule; the future deployment must supply the corresponding
execution or payment mechanism. This signer proves the event, call hash,
successful single-call outcome, exact negative ether delta, roots, and DA
bytes; it does not prove how the deployment obtains or safeguards those funds.
This profile therefore adds no EIP-7702 authorization, account-code-hash,
balance, or block-beneficiary gate. The existing system-sender and
reserved-source restrictions remain transaction classification rules, not a
proof of the deployment's funding mechanism.

This correspondence proves L2 origin: the exact re-executed transaction
produced, from the fixed EEZL2 address, a canonical event carrying the call
hash that commits target, value, data, source, both rollup directions, and
manager-entry gas. A flat list or multiset of hashes is insufficient because it loses the
transaction and receipt-log provenance retained here. Section 8.8 additionally
binds the derivation sidecar and exact `[load, user]` bytes to the same
re-executed block.

The outbound entry's success-committing `rollingHash` and `returnData` remain
expected L1 predicates, not a claim that the signer observed a future L1
execution. The signature commits the complete entry, and EEZ executes its exact
target, value, data, and source proxy inside a revertible entry frame. EEZ recomputes
the rolling hash from the actual success and return bytes and requires the
live root, complete call consumption, and exact ether-delta equation before
committing the StateDelta. Any mismatch reverts that whole frame — including
the target's effects and value transfer. On the immediate path, the surrounding
catch advances to later entries after that rollback. On the deferred path, the
consumption transaction reverts and its queue-cursor update reverts with it, so
the entry remains pending. Admission therefore authorizes a conditionally
applicable outbound transition; it does not promise successful L1 application,
dispatch mode, or a globally applied prefix. Each entry remains independently
root-gated: a later entry normally becomes stale when its predecessor does not
apply, but it can still apply if the live root equals its `currentState`, for
example when roots repeat or the chain later returns to that value. This is a
liveness/reconciliation case, not permission for a partial effect within an
entry.

The unsigned transient counts may change immediate-versus-deferred scheduling
after signing and MUST NOT be used as authorization evidence. Both consumption
paths remain subject to EEZ's atomic root, rolling-hash, call-consumption, and
ether checks. A future profile that promises a schedule or successful outcome
must first bind that information in the public-input preimage or another
authoritative L1 mechanism.

Interpreting the fixed address as canonical EEZL2 depends on protocol trust in
its deployed code or on a separate code-identity guarantee; this gate does not
verify its code hash. Deployments MUST preserve that identity through their
root and upgrade policy. Explicit code-identity pinning remains future
hardening.

### 8.7 Ether-delta consistency

For every batch, require the leading `Anchor`'s sole StateDelta to have
`etherDelta == 0`. A canonical anchor receives no inbound value and executes
no outgoing calls, so applying a non-zero delta makes
`EEZ._applyAndExecute` revert with `EtherDeltaMismatch`. When dispatched as
the intended leading immediate entry, the surrounding catch skips both the
anchor and its state transition.

The outbound accounting rule is part of §8.6's active single-call
success-expected profile. Convert the sole call's `uint256 value` to a
non-negative `int256` with checked conversion, negate it, and require the sole
StateDelta's `etherDelta` to equal that result exactly. A value outside the
non-negative representable `int256` range or any delta mismatch rejects. Zero
value does not relax the entry-shape, source, event, or rolling-hash checks.
Failed, multi-call, nested-table, and non-zero-`revertSpan` outbound entries
remain unsupported.

Every provisionally `Inbound` effect MUST be bound to the strict successful
observation described in §8.5 and MUST have
`etherDelta == int256(value)`. Conversion from the equality-checked `uint256`
value MUST be checked; values outside the non-negative representable Solidity
`int256` range reject. Failed inbound calls remain unsupported and MUST NOT be
assigned this positive-value rule.

Reject either direction when the required signed value is outside its stated
Solidity `int256` range. This gate checks accounting consistency; for outbound
it validates the success-expected negative-value predicate that L1 later
enforces atomically. It does not independently predict or observe the future
L1 target call or the L2 funding mechanism.

### 8.8 DA sidecar and system-transaction binding

`B.callData` is composer-controlled data used by followers to reconstruct
the posted L2 block range. Including `H(callData)` in the public-input hash
only commits the signature to those bytes; it does not prove that they encode
the blocks re-executed by this daemon. After §§8.4–8.7 have authorized every
effect, the active profile MUST enforce the following payload before
attestation:

```text
0x00 || rlp([blockTxCounts, transactions, l2Entries])
```

Decode it with the signer's local exact RLP parser. The parser MUST retain
borrowed slices and compare their elements directly instead of allocating
vectors from composer-controlled list lengths. Then require all of the
following:

1. the RLP item after the tag is canonical and consumes every remaining byte;
2. `sum(blockTxCounts) == transactions.len()`;
3. `blockTxCounts.len() == to_block - from_block + 1`, matching the admitted
   inclusive window exactly;
4. position `j` describes validated block `from_block + j`, without sorting,
   deduplication, omission, or reordering;
5. every intermediate block's count and transaction list equal its consensus
   transactions exactly, using raw EIP-2718 bytes in block order;
6. the terminal block's count and transaction list omit **only** the exact
   system transactions reconstructed below: each outbound `load` half and
   each inbound delivery transaction. Every outbound user transaction remains
   present, and every retained transaction equals its consensus raw EIP-2718
   bytes in order; and
7. `l2Entries` contains exactly one ordered canonical sidecar for every
   effect, first every outbound sidecar in effect order and then every inbound
   sidecar in effect order, with no missing, extra, reordered, or mutated
   entry.

Every outbound effect MUST precede every inbound effect in the retained effect
plan; an outbound-after-inbound plan is not a canonical Sync-block order and
MUST reject. For each outbound binding, derive its canonical derivation
sidecar by copying the matched batch `ExecutionEntrySol`, clearing
`stateDeltas`, and leaving every other field unchanged. ABI-encode that
projection and require the corresponding leading `l2Entries` item to equal
those bytes exactly. The projection deliberately omits the settlement state
deltas: followers use the entry to reconstruct the Sync block, while the full
batch entry remains subject to the independent state-chain and accounting
checks in §§8.3–8.7.

For each admitted inbound observation, derive the canonical sidecar as the ABI
encoding of an `ExecutionEntrySol` with:

- no state deltas;
- the recomputed non-zero `proxyEntryHash` and configured destination rollup;
- exactly one `l2ToL1Call` equal to the strict inner call, including target,
  value, data, source address, source rollup, and zero revert span;
- empty expected inbound calls and lookups;
- `callCount == 1`; and
- the exact return data and rolling hash from the strict observation.

The corresponding payload `l2Entries` item, after all outbound sidecars, MUST
equal those canonical ABI bytes exactly.

The payload omits the system-signed half of each effect even though those bytes
contribute to the L2 transaction root. For a non-empty effect prefix, the
daemon MUST use the same canonical projections required of the verified
sidecars, pair each outbound projection with its retained raw user transaction,
and call the canonical
`build_cross_chain_sync_pairs`, followed by `interleave_sync_block_txs`.
Given `K` outbound effects, `M` inbound effects, and starting nonce `N`, the
resulting full terminal sequence is exactly:

```text
[load(outbound[0], N), user[0],
 ...,
 load(outbound[K-1], N+K-1), user[K-1],
 deliver(inbound[0], N+K),
 ...,
 deliver(inbound[M-1], N+K+M-1)]
```

The generated sequence's length and every raw EIP-2718 transaction byte MUST
equal the complete re-executed terminal block in order. This single comparison
binds each outbound sidecar, its immediately preceding load, its retained user
transaction, every inbound delivery, and the outbound-then-inbound nonce order.
Reconstruction uses:

- the mandatory system private key whose public address is exactly
  `SYSTEM_ADDRESS`;
- the operator-configured ChainSpec chain id and configured rollup id;
- the canonical EEZL2 address;
- legacy EIP-155 transactions with gas price `1_000_000_000` and gas limit
  `2_000_000`; and
- the nonce of the first authenticated system transaction in the re-executed
  terminal block, incremented once per generated system transaction.

Stateless validation has already proved that this starting nonce was valid in
the re-executed state. Deriving it from an inbound delivery would be wrong for
a mixed block because its nonce follows all outbound loads. Equality of
calldata or recovered sender alone would not be enough: another valid ECDSA
signature changes the raw transaction and may change the transaction root and
block hash. A change to either canonical builder, transaction type, or fixed
constants is therefore a profile revision, not a transparent implementation
detail.

The parser MUST stop after the exact validated block and transaction counts,
reject any remaining list content, and never materialize payload transaction
or L2-entry vectors. It also MUST reject bytes after the single outer RLP
item. Once the validated budget is exhausted, surplus or otherwise forbidden
content is rejected immediately without parsing its interior. A malformed
payload within the admitted prefix is an invalid argument; content beyond that
prefix or a canonical payload that disagrees with the validated window is a
failed precondition.

The effect-free anchor case is the natural zero-element form of these rules:
no transaction is omitted and `l2Entries` is empty. An implementation MUST NOT
obtain wider effect support by generically filtering system transactions or by
merely accepting non-empty `l2Entries`.

For outbound, these checks prove that `callData` carries the exact derivation
projection used to construct the same raw `[load, user]` transaction pair that
Stateless validated. They do not prove that the future L1 target call succeeds,
that claimed return data came from L1 execution, that the batch will apply to
the live L1 state, or that the pinned EEZL2 address has an independently
authenticated code identity. Those are not attestation claims: §8.6 defines
the conditional L1-application semantics and the trusted-code assumption.

## 9. Attestation

Version 9 MUST reach this step after every applicable gate accepts an
effect-free anchor batch or a batch whose effects are successful inbound and/or
single-call success-expected outbound bindings authorized by §§8.5–8.8. Failed
inbound, lookup-bearing inbound, and unsupported outbound shapes remain
pre-signing refusals.

After all applicable gates pass, sign the recomputed 32-byte hash directly
with secp256k1. Do not add an EIP-191 prefix or another hash.

Signature encoding is 65 bytes:

```text
r[32] || low_s[32] || v[1], where v = 27 + recovery_id
```

`s` MUST be low-s normalized; a recovery id above 1 is a signing error (never
emit `v` of 29/30).

Return:

```text
ProveResponse {
    public_inputs_hash: recomputed 32-byte hash,
    signature:          65-byte signature
}
```

A signing error refuses the RPC like any other failure. The composer installs
the signature into the batch's `proofs` after this daemon attests; `B.proofs`
is therefore ignored on input.

The signature is produced by the mandatory configured signer and is intended
for the mandatory configured `proof-system`; the public-input hash uses the
independently configured `vkey`. These values MUST NOT be derived from one
another. Section 3 defines the operator's responsibility to bind all three to
the same L1 deployment.

An attestation proves only that the supplied transition and admitted batch
passed this profile from `window_pre_state_root`. It does not prove
canonical L2 ancestry, sequencer authorization, current L1 applicability,
successful application, or immediate-versus-deferred scheduling. Those limits
follow directly from §7.4 and the unsigned transient counts.

## 10. Failure rules

Fatal startup errors in the active profile:

- invalid or missing required core or `stateless` configuration, including
  `vkey`, `proof-system`, `signer-key`, `attester-address`, or `l2-system-key`;
- a zero or invalid proof-system address, or an invalid secp256k1 private
  scalar, including an attestation key that derives `SYSTEM_ADDRESS`, an
  attestation key that does not match `attester-address`, or an L2 system key
  that does not derive it;
- a zero timeout or one that cannot form a monotonic-clock deadline; or
- listener bind failure.

A future backend-selection profile additionally makes its selected backend's
missing configuration startup-fatal.

Per-RPC refusals — a gRPC error status and no signature:

- a stream-discipline violation, including a missing block witness
  (section 4);
- a quota, timeout, or overlapping-request admission refusal (sections 3, 4);
- a header with `from_block == 0` or `from_block > to_block`;
- a header `rollup_id` that differs from the configured `rollup-id`;
- a window-admission violation: count mismatch in either direction, gap,
  duplicate, reordering, malformed hash length, or hash-chain break
  (section 6);
- backend rejection, backend output that violates section 7's rules, or a
  sub-chunk telescope mismatch;
- any settlement-gate rejection, including a batch proof-system address that
  differs from operator configuration (section 8); or
- a signing error (section 9).

A refusal MUST NOT persist protocol or validation state that changes a later
admitted RPC's judgment: the same window is then evaluated from scratch. A
timed-out non-interruptible worker MAY transiently retain the sole admission
slot, so an immediate retry can receive `resource_exhausted` until that worker
exits. Retry timing belongs entirely to the composer.

## 11. Conformance checklist

A conforming implementation MUST pass tests covering:

1. operator configuration: missing, empty, malformed, out-of-range, and zero
   `rollup-id` startup failures, plus CLI-over-environment precedence; required
   non-zero `vkey`, required non-zero `proof-system`, and required valid
   `signer-key`, `attester-address`, and `l2-system-key`, including malformed,
   zero, out-of-range, reserved-attestation-address, mismatched-attester-address,
   and wrong-system-address private scalars;
   prove that the configured vkey remains independent from the derived
   attestation signer address and that help, errors, `Debug`, and logs never
   expose either private key; verify the
   checkpoint-limit default and its accepted zero value, and
   reject its malformed or out-of-range values; reject timeout values that
   cannot form a monotonic-clock deadline; accept complete `Genesis` and
   bare `ChainConfig` documents, and deterministically reject unsupported
   top-level or nested chain-configuration fields (section 3);
2. stream discipline: header-first, duplicate header, block-first, kindless
   chunk, empty stream, missing `post_batch`, and missing block-witness
   refusals, plus one refusal per quota kind, including
   `resource_exhausted` for a checkpoint selection above its limit; require
   request-stream EOF after the declared span; reject an overlapping request
   across connections without queueing it; prove that a timed-out running
   worker retains admission until cooperative exit and that graceful shutdown
   waits for that exit (sections 3–4);
3. window admission: bounds, a configured-rollup header mismatch that returns
   `failed_precondition` immediately without waiting for further chunks or
   invoking the backend, count mismatch in both directions, gap, duplicate,
   reordering, malformed hash lengths, hash-chain break, and the single-block
   window (sections 5–6);
4. backend-output rules: block-count and per-index computed-hash cross-checks;
   one associated `BackendBlockOutput` per admitted block; exact receipt-success
   and system-sender coverage; coordinate-ordered outbound observations with
   in-range transaction indices; one
   post-state root per block with `window_post_state_root` derived from the
   settling output; sparse transaction-checkpoint ordering, uniqueness, and
   bounds; post-block endpoint independence; empty preceding checkpoint
   vectors; the active Stateless exact-selection contract (settling `[]` for an
   empty selection or the exact ordered selected checkpoints otherwise); and
   rejection of an empty window (section 7);
5. backend conformance: the fixture bundle below reproduced through the
   backend under test from its native inputs; for the active local adapter,
   cover an empty final selection, successful non-empty selection with exact
   independently recorded positions and roots, a zero limit, the exact quota
   boundary and quota-plus-one fail-fast behavior, missing, extra, or reordered
   backend checkpoint positions, a locally recovered system/user position
   pattern, associated settlement-evidence binding for both settling and
   preceding blocks, and the pre-/post-Homestead EIP-2 recovery boundary;
6. every active public-input, state-chain, effect-prefix, successful-inbound
   authorization, single-call success-expected outbound authorization, anchor
   ether-delta, system-status, and DA-sidecar rejection case — including
   zero or multiple rollup-assignment rows, an assignment row whose id differs
   from the configured rollup, a sole proof-system address that differs
   from operator configuration, any proof-system-index array other than exactly
   `[0]`, non-zero `crossProofSystemInteractions`, and a decoded
   `settled_rollup` that differs from the
   configured rollup even when the header and assignment row match; a
   `SYSTEM_ADDRESS` signer or raw type-`0x7E` transaction in a pre-settling
   block, a settling-block `SYSTEM_ADDRESS` signer targeting anything other
   than canonical EEZL2, selector-spoofed ordinary transactions, exact inbound candidates,
   successful non-empty inbound carriers, failed or lookup-bearing inbound,
   and malformed or trailing settling-block RLP;
   prove that changing either transient count neither changes the recomputed
   public-input hash nor causes signer admission to reject; for the effect
   prefix, cover one- and multi-block pre-settling roots, positional root and
   kind matching, duplicate root values, and missing, extra, reordered, or
   out-of-range checkpoints, plus an inbound candidate hidden before an
   outbound-looking effect candidate; for inbound binding, cover missing,
   extra, invalid, failed, and reordered observations, duplicate hashes with
   distinct return data, non-canonical deferred-entry fields, a wrong
   destination, hash, return data, or ether delta, and a value outside the
   non-negative representable `int256` range; for outbound observations, cover
   emitter and signature filtering, strict event decoding, receipt-local
   positions, preserved duplicate hashes, zero manager-entry gas, rejection of
   non-zero manager-entry gas, intermediate-block events, and
   missing, extra, multiple, malformed, reassociated, wrong-rollup, or
   wrong-hash correspondence; additionally reject the reserved system source,
   an outbound value outside the non-negative `int256` range, non-unit `callCount`,
   non-empty expected-call or expected-lookup tables, non-zero `revertSpan`,
   altered return data, malformed or failed rolling outcomes, and every
   outbound ether delta other than exact `-int256(value)`; prove that fully
   matched zero-value and non-zero-value outbounds pass their exact DA binding
   and return the recomputed attestation input; for `callData`, cover malformed
   and trailing payloads, both block-count mismatch directions, per-block count
   redistribution, exact legacy and typed EIP-2718 bytes, substitution or
   reordering, preservation of ordinary intermediate transactions, omission
   of only the bound terminal system positions while retaining every outbound
   user transaction, and missing, extra, reordered, or mutated `l2Entries`;
   cover the outbound projection with `stateDeltas` removed and reject a
   sidecar that retains or otherwise changes them; verify exact full raw
   reconstruction for outbound-only, inbound-only, and mixed
   outbound-then-inbound sequences, including a non-zero starting nonce, and
   reject an outbound-after-inbound effect order, a missing or moved load, or
   any transaction-type, chain-id, nonce, gas, destination, value, calldata,
   or signature mismatch; prove with distinct non-zero roots
   that `currentState` and `newState` both affect the recomputed hash and are
   matched to the re-executed parent and final roots; prove that a malformed or
   false composer `public_inputs_hash` claim is ignored and never controls the
   returned hash; and
7. active attestation: exact 32-byte recomputed hash and 65-byte raw-digest
   signature in `ProveResponse`, low-`s`, `v` in `{27, 28}`, recovery to the
   configured signer's derived address without an EIP-191 prefix, signing
   failure with no response, and no signature for every rejection path; include
   an L1 integration fixture in which the configured proof system, independent
   vkey, and authorized signer agree and only the first applicable of two valid
   sibling transitions mutates state. Include a successful-inbound conformance
   fixture captured from the current protocol and require its expected
   public-input hash and valid recovered signature. The active profile MUST cover an admitted canonical
   outbound and mixed outbound-then-inbound sequence while proving that every
   malformed origin, shape, accounting, sidecar, transaction, nonce, or order
   is rejected before signing (sections 7.4 and 8.6–9). The active test profile
   MUST also run a canonical non-zero-value outbound case from a real witness
   through Stateless receipt extraction, settlement, and signature recovery.

The crate-local fixture
`tests/fixtures/fresh-chain-inbound-2175/` uses the superseded call-hash
preimage and is retained as a fail-closed regression. It preserves every
transaction-bearing block body from the recorded `[1561..2175]` window, the
exact `PostBatch`, independently recorded checkpoints and artifact hashes, and
the expected public-input hash. Empty-body positions are represented by an
equivalent empty block body because consensus validation is outside this
projection. The dedicated Stateless adapter fixture covers consensus validation
separately. A conforming implementation MUST reject the fixture when its
claimed inbound hash is compared with the Annex B recomputation and MUST NOT
sign it.

This fixture spans 615 blocks, while the reference service default is 512. A
conformance test that sends it through the full RPC and expects success MUST
raise `max-request-blocks` to at least 615; a direct validation-and-settlement pipeline
test is not subject to stream-admission quotas.

The crate-local fixture `tests/fixtures/nonzero-outbound-630/` is an
incompatible-event rejection vector. Its captured L2 manager emitted the
five-field base `CrossChainCallExecuted` event, while the current `EEZL2`
contract emits the six-field overload containing `uint64 callGas`. A conforming
implementation MUST NOT treat those logs as current outbound evidence or
invent a zero gas value, and therefore MUST NOT sign this captured batch.

Golden vectors: the reference repository ships
`crates/eez-proof-signer/tests/fixtures/stateless-block-13/` — a real captured
block, its augmented witness, its chain configuration, and its posted batch.
For `postbatch-13.json`, the exact public-input vector is:

```text
vkey = 0x000000000000000000000000f39fd6e51aad88f6f4ce6ab8827279cfffb92266
l1_block_hash = absent
publicInputsHash = 0xe5cd0221135432a8f42b61e68f71f809d7e9b973c6866da2446fca8dd1339c98
```

A conforming implementation MUST reproduce that hash and accept the fixture's
settlement chain. The Foundry-generated public-input suite
`crates/eez-protocol/tests/fixtures/public_inputs_hash_vectors.json` contains
seven vectors. A conforming implementation MUST match every expected hash;
these cover the general Annex C fold. Annex B is locked against
`contracts/test/CallHashVectors.t.sol` by the unit tests in
`eez_protocol::action`, including mutable, static, and `uint64` boundary
vectors. The public-input vectors operate on precomputed entry, lookup,
and blob hashes and deliberately include generic multi-PS and non-empty-blob
cases. Test that low-level fold against all seven vectors; the daemon-level
admission rules in §8.1 still reject unsupported batch shapes.

The Foundry-generated entry-rolling-hash vectors are in
`crates/eez-protocol/tests/fixtures/rolling_hash_vectors.json`, with
operation scripts in `crates/eez-protocol/tests/rolling_hash_vectors.rs`. A
conforming Annex D implementation MUST match all five `entry` vectors.

---

## Annex A. `postAndVerifyBatch` calldata (batch decoding)

`PostBatch.abi_calldata` is complete Solidity **calldata** for
`EEZ.postAndVerifyBatch` — selector `0x8b1a095a` followed by the standard ABI
encoding of the single struct argument. Decoding MUST verify the selector,
ABI-decode the argument, re-encode it with
`eez_protocol::entries::encode_postbatch`, and require byte-for-byte equality with
the input. This exact round trip rejects trailing bytes and non-canonical ABI
representations. The types (field order is normative — it is the ABI layout):

```solidity
struct StateDeltaSol {
    uint256 rollupId;
    bytes32 currentState;
    bytes32 newState;
    int256  etherDelta;
}

struct L2ToL1CallSol {
    address targetAddress;
    uint256 value;
    bytes   data;
    address sourceAddress;
    uint256 sourceRollupId;
    uint256 revertSpan;
}

struct ExpectedL1ToL2CallSol {
    bytes32 crossChainCallHash;
    uint256 callCount;
    bytes   returnData;
}

struct ExpectedLookupSol {
    bytes32                 crossChainCallHash;
    bytes                   returnData;
    bool                    failed;
    uint64                  l2ToL1CallNumber;
    uint64                  lastL1ToL2CallConsumed;
    uint64                  executingLookupIndex;
    L2ToL1CallSol[]         l2ToL1Calls;
    ExpectedL1ToL2CallSol[] expectedL1ToL2Calls;
    uint256                 callCount;
    bytes32                 rollingHash;
}

struct ExpectedStateRootPerRollupSol {
    uint256 rollupId;
    bytes32 stateRoot;
}

struct ExecutionEntrySol {
    StateDeltaSol[]         stateDeltas;
    bytes32                 proxyEntryHash;
    uint256                 destinationRollupId;
    L2ToL1CallSol[]         l2ToL1Calls;
    ExpectedL1ToL2CallSol[] expectedL1ToL2Calls;
    ExpectedLookupSol[]     expectedLookups;
    uint256                 callCount;
    bytes                   returnData;
    bytes32                 rollingHash;
}

struct LookupCallSol {
    bytes32                         crossChainCallHash;
    uint256                         destinationRollupId;
    bytes                           returnData;
    bool                            failed;
    L2ToL1CallSol[]                 l2ToL1Calls;
    ExpectedL1ToL2CallSol[]         expectedL1ToL2Calls;
    ExpectedLookupSol[]             expectedLookups;
    uint256                         callCount;
    bytes32                         rollingHash;
    ExpectedStateRootPerRollupSol[] expectedStateRoots;
}

struct RollupIdWithProofSystemsSol {
    uint256  rollupId;
    uint64[] proofSystemIndex;
}

struct ProofSystemBatchPerVerificationEntriesSol {
    ExecutionEntrySol[]           entries;
    LookupCallSol[]               l1ToL2lookupCalls;
    uint256                       transientExecutionEntryCount;
    uint256                       transientLookupCallCount;
    address[]                     proofSystems;
    RollupIdWithProofSystemsSol[] rollupIdsWithProofSystems;
    bytes32                       crossProofSystemInteractions;
    uint256[]                     blobIndices;
    bytes                         callData;
    bytes[]                       proofs;
    uint64                        blockNumber;
}

function postAndVerifyBatch(ProofSystemBatchPerVerificationEntriesSol batch) external;
```

Spec references of the form `B.X` denote field `X` of the decoded
struct.

## Annex B. `common_cross_chain_call_hash`

```text
common_cross_chain_call_hash(mode, sourceAddress, sourceRollupId,
                             targetAddress, targetRollupId, value, data)
  = keccak256(abi.encode(mode == Static, sourceAddress,
                         uint64(sourceRollupId), targetAddress,
                         uint64(targetRollupId), value, data))
```

where `abi.encode(a, b, ...)` is Solidity's **parameter-list encoding** — the
head/tail encoding of the seven values as a parameter list, with **no** leading
32-byte tuple-offset word. Rollup ids are Solidity `uint64` values. This is
byte-identical to `EEZBase.computeCrossChainCallHash`. Mutable calls leaving an
L2 use the distinct gas-aware `EEZL2.computeCrossChainCallHash` overload; L2
static calls use this common gas-free formula.

## Annex C. Public-input hashes

Let `H(x) = keccak256(x)`. All `abi.encode` operations below use the Solidity
types and field order from Annex A; `||` is byte concatenation. For daemon
admission, require the following before invoking the hash fold:

- exactly one proof system (§8.1), whose address equals the configured
  non-zero configured `proof-system`;
- exactly one `rollupIdsWithProofSystems` row;
- that sole row's `rollupId` fits in `u64`, is non-zero, and equals the
  configured `rollup-id`;
- that sole row's complete `proofSystemIndex` array is exactly `[0]`;
- every entry `destinationRollupId`, every entry `StateDelta.rollupId`, and
  every top-level lookup `destinationRollupId` equals that sole row's
  `rollupId`;
- `crossProofSystemInteractions == bytes32(0)` in this single-proof-system
  profile;
- `blockNumber == 0`, no `l1_block_hash`, and empty `blobIndices`.

Reject any violation; never narrow an out-of-range `uint256` by truncation or
a panicking conversion. The timeless context for every rollup row is
`timestamp = uint256(0)` and `blockHash = bytes32(0)`. The same independently
configured `vkey` is used for every row's selected proof system. Do not constrain the
received `proofs` array: it is excluded from this hash and populated after
signing. The two transient counts are also excluded from the deployed hash.
They are unsigned L1 scheduler inputs, so the signer MUST NOT constrain their
values or add them to the preimage.

Compute:

```text
entryHashes[i] = H(abi.encode(B.entries[i]))
lookupHashes[i] = H(abi.encode(B.l1ToL2lookupCalls[i]))
blobHashes = []

shared = H(
    abi.encode(entryHashes)
 || abi.encode(lookupHashes)
 || abi.encode(blobHashes)
 || H(B.callData)
 || B.crossProofSystemInteractions
)
```

The concatenation inside `shared` is Solidity
`abi.encodePacked(abi.encode(...), abi.encode(...), abi.encode(...), bytes32,
bytes32)`. In particular, each dynamic array retains its own ABI
offset/length encoding; this is not a flat concatenation of its elements.

For each proof-system index `k` (currently only `0`), initialize
`acc = bytes32(0)` and visit rollup rows in encoded order (daemon admission
currently permits exactly one). If row `r` contains `k` at local position
`j`, set:

```text
acc = H(abi.encode(
    acc,
    uint256(row[r].rollupId),
    bytes32(vkey),
    bytes32(0),          // blockHash
    uint256(0)           // timestamp
))
```

The result for `k` is:

```text
publicInputsHash[k] = H(shared || acc)
```

Here `shared || acc` is exactly 64 bytes. Section 11 pins the complete
fixture input and expected output.

## Annex D. Inbound decoding (`decode_inbound`)

A genuine inbound delivery uses Solidity calldata (selector `0xeb494246` +
ABI arguments) from the following complete type family. Field order is
normative. The low-level decoder is applied to transaction input and does not
itself check the transaction signer, recipient, native value, receipt status,
or `sourceRollup`. It also does not establish equality between the explicit
outer call arguments and the inner call that EEZL2 executes. The decoder is
usable for outcome recovery only after §8.5 has established the strict
canonical envelope, outer/inner equality, successful receipt, call hash, and
positional effect binding; §§8.2 and 8.4 define system classification and
effect ordering.

```solidity
struct CrossChainCallSol {
    address targetAddress;
    uint256 value;
    bytes   data;
    address sourceAddress;
    uint256 sourceRollupId;
    uint256 revertSpan;
}

struct ExpectedOutgoingCrossChainCallSol {
    bytes32 crossChainCallHash;
    uint256 callCount;
    bytes   returnData;
}

struct L2ExpectedLookupSol {
    bytes32                             crossChainCallHash;
    bytes                               returnData;
    bool                                failed;
    uint64                              callNumber;
    uint64                              lastOutgoingCallConsumed;
    uint64                              executingLookupIndex;
    CrossChainCallSol[]                 incomingCalls;
    ExpectedOutgoingCrossChainCallSol[] expectedOutgoingCalls;
    uint256                             callCount;
    bytes32                             rollingHash;
}

struct L2ExecutionEntrySol {
    bytes32                             proxyEntryHash;
    CrossChainCallSol[]                 incomingCalls;
    ExpectedOutgoingCrossChainCallSol[] expectedOutgoingCalls;
    L2ExpectedLookupSol[]               expectedLookups;
    uint256                             callCount;
    bytes                               returnData;
    bytes32                             rollingHash;
}

struct L2LookupCallSol {
    bytes32                             crossChainCallHash;
    bytes                               returnData;
    bool                                failed;
    CrossChainCallSol[]                 incomingCalls;
    ExpectedOutgoingCrossChainCallSol[] expectedOutgoingCalls;
    L2ExpectedLookupSol[]               expectedLookups;
    uint256                             callCount;
    bytes32                             rollingHash;
}

function executeIncomingCrossChainCall(
    address destination,
    uint256 value,
    bytes   data,
    address sourceAddress,
    uint256 sourceRollup,
    L2ExecutionEntrySol[] entries,
    L2LookupCallSol[]     lookupCalls
) external payable returns (bytes);
```

Decoding a transaction yields `DecodedInbound { target = destination, value,
data, source = sourceAddress, return_data, success }`. The first four fields
come from the explicit outer arguments, `return_data` is
`entries[0].returnData`, and `success` is re-derived from that entry's rolling
hash. These fields are extracted from one sealed transaction, but the decoder
does not prove that they all describe one executed inner call. Let `U1` be the
32-byte big-endian encoding of integer 1 and let `Z` be 32 zero bytes. For
`s ∈ {true, false}`:

```text
r1   = keccak256(Z || 0x01 || U1)
r2_s = keccak256(r1 || 0x02 || U1 || bool8(s) || return_data)
bool8(true) = 0x01; bool8(false) = 0x00
```

This is Solidity `abi.encodePacked`; `return_data` is raw bytes with no
length prefix. Try `true` before `false` and take the first `s` for which
`r2_s == entries[0].rollingHash`. The low-level decoder returns no value when
the ABI does not decode, `entries` is empty, or neither flag matches. This
recognizer alone remains non-authoritative: additional entries and all
`lookupCalls` are decoded but ignored, and it does not compare
`entries[0].incomingCalls[0]` with the outer arguments or constrain
`revertSpan`. Section 8.5 admits its recovered outcome only after separately
requiring the complete canonical single-entry shape, outer/inner equality,
successful receipt, call-hash identity, and positional settlement binding.
Selector-shaped ordinary transactions are not inbound candidates.

## Annex E. ZisK backend (`native-validate`; draft and inactive)

The current binary does not implement or select this backend and does not
accept its options. This annex preserves the target subprocess contract for a
future profile.

As written, the subprocess summary below transports only the scalar and
per-block execution fields needed to populate part of §7.1. It does not
transport `blocks[i].settlement_evidence`: the exact fork-aware system-sender
flags and receipt-derived outbound-event observations required by the active
settlement profile. A future activation MUST define and authenticate that
associated evidence from the same execution (or define a different settlement
profile); host-side signer recovery and block RLP alone cannot recover verified
receipt logs and are not a substitute. Therefore this draft is not yet a
sufficient selectable-backend contract even when every summary field below is
present.

That profile produces its partial section-7 backend output by staging each
window for the ZisK `native-validate` subprocess. Its options: `--validator-bin` /
`EEZ_VALIDATOR_BIN` (required), `--chain-config` / `EEZ_CHAIN_CONFIG`
(required; the bare Alloy `ChainConfig` object, equivalent to `genesis.json`'s
`.config`, not the complete genesis document), and `--work-dir` /
`EEZ_VALIDATOR_WORKDIR` (default `/tmp/eez-proof-signer`).

Stage each window in a fresh, empty per-window directory under `work-dir`.
The directory MUST be created empty (remove any pre-existing directory of the
same name before staging); its name is not part of the contract — only the
file names inside are. For every block at height `n` (heights, not indices),
write:

```text
block-<n>.rlp       exact BlockWitness.rlp bytes
witness-<n>.json    witness arrays as 0x-prefixed lowercase hex strings
```

Witness JSON shape (exactly these four keys, nothing else):

```json
{"state":["0x..."],"codes":["0x..."],"keys":["0x..."],"headers":["0x..."]}
```

Invoke, with no additional arguments, environment changes, or stdin:

```text
<validator-bin> <chain-config> --dir <window-directory>
```

A missing witness file, spawn error, or non-zero exit rejects the window
before the backend claims success. After a zero exit, parse the last stdout
line whose first non-whitespace character is `{` (stderr is ignored). Absence
of such a line, invalid JSON, a missing required field, or any violated
backend-output rule below is an invalid successful backend result and maps to
`internal`, not to backend rejection. Root and hash values are JSON strings
holding 32 bytes of hex, accepted case-insensitively with an optional `0x`
prefix. Unknown extra fields are ignored.

The summary populates the partial `BackendWindowOutput` as:

| Summary field | Backend output field or check |
| --- | --- |
| `parent_state_root` (required) | `pre_state_root` |
| `final_state_root` (required) | MUST equal the last `blocks[i].post_state_root`; it is not retained as a separate field |
| `blocks[i].hash` (required) | `blocks[i].computed_hash` |
| `blocks[i].tx_statuses` (required, exact transaction coverage) | `blocks[i].receipt_successes` |
| `blocks[i].state_root` (required) | `blocks[i].post_state_root` |
| `blocks[i].transaction_state_checkpoints` (required vector, indexed as in §7.1) | `blocks[i].transaction_state_checkpoints` |

The adapter exact-decodes each materialized block RLP to populate
`decoded_number`, `decoded_parent_hash`, and `decoded_transaction_count` before
the section-7 association checks.

The section-7 association, length, hash, endpoint, and checkpoint rules apply
to the populated output as usual. The absent settlement evidence still prevents
selection under the active profile.

*Reference note:* the ZisK toolchain is pinned at
`eez-association/zisk-eth-client` commit
`a2dd9ac41012e5991d5938bf096c0fd2885ab1e0` and
`eez-association/zisk-patch-stateless` commit
`14fa34607d2ae2a9ce7edb3f0d1a50bc7ea8c474`. The subprocess SHOULD be spawned
kill-on-drop so a cancelled RPC does not leak a running validator.
