# Unbound scheduling counts: accepted protocol semantics

Status: reviewed with the protocol owners; intentional dispatch policy

## Executive conclusion

At protocol revision `6fcc90b65063831cb7797e9fa361004064d28f9f`,
`immediateEntryCount` and `immediateStaticEntryCount` are not included in the
public-input hash. They can therefore be changed after a proof or ECDSA
attestation has been created without invalidating that proof or signature.
Those fields nevertheless control whether proved entries are executed in the
posting transaction, exposed transiently to the poster's meta hook, persisted
in a queue, or left unavailable after the transaction.

The mechanism and its observable effect are confirmed. A focused adversarial
test demonstrates that changing only `immediateEntryCount` preserves the exact
public-input hash while changing a deferred execution queue from one entry to
zero.

This is **not an implementation bug relative to the current protocol**. The
interface and protocol specifications explicitly call both fields unproven
dispatch parameters and permit the split to be re-tuned without re-proving.
The protocol owners have confirmed that this is intentional: proofs authorize
entry correctness, while the poster chooses which eligible non-leading entries
to make immediately available through its meta hook and may choose not to
execute the remainder. The security documentation likewise treats discarding
unconsumed proved entries as a liveness choice rather than a state-safety
failure.

The appropriate classification is therefore:

| Question | Conclusion |
| --- | --- |
| Are the scheduling counts absent from the proof hash? | Confirmed |
| Can changing a count preserve proof validity but change queue availability? | Confirmed by test |
| Can an EOA poster cause a proved non-L2Tx entry to be neither executed nor queued? | Confirmed by code and test |
| Is the static-entry count affected by the same construction? | Confirmed by code; no equivalent adversarial A/B test yet |
| Does this permit arbitrary entry contents or unproved state transitions? | No evidence of that; runtime safety gates remain active |
| Does the implementation violate the current protocol specification? | No; the behavior is explicitly specified |
| Does the protocol guarantee that every proved entry is executed or remains available? | No; scheduling and availability beyond the mandatory leading L2-transaction run are delegated to the poster |

This report records the behavior as an **accepted trust and liveness tradeoff**.
It would become a protocol vulnerability only for a deployment that promises
scheduling integrity or atomic availability without adding a separate policy
that enforces those properties.

## Scope and method

The assessment covered:

- the pinned `EEZ` contract and proof-system interfaces;
- the public-input construction and every consumer of both scheduling counts;
- structural, state-root, rolling-hash, and ether-flow backstops;
- the core, multi-prover, execution-entry, lookup, and proof-signer
  specifications;
- protocol history around the scheduling boundary check;
- upstream protocol tests and the repository's adversarial public-input test;
- the Composer and proof-signer settings that determine whether the scenario
  is reachable in the currently supported profile.

No protocol source was modified as part of this assessment.

## Confirmed mechanism

### 1. The counts are independent calldata fields

The batch carries `immediateEntryCount` and `immediateStaticEntryCount` as
separate fields. The interface itself labels them "UNPROVEN dispatch params"
and says that the split can be re-tuned without re-proving
([`IEEZ.sol:25-44`](../../../sync-rollups-protocol/src/interfaces/IEEZ.sol#L25)).

### 2. The proof hash binds entry contents, but not the counts

`EEZ._verifyProofSystemBatch` hashes every complete execution entry and static
entry ([`EEZ.sol:683-695`](../../../sync-rollups-protocol/src/EEZ.sol#L683)).
Its shared preimage then contains:

- the entry hashes;
- the static-entry hashes;
- selected blob hashes;
- `keccak256(batch.callData)`;
- per-rollup custom-data hashes; and
- the bound sender or `address(0)`.

Neither scheduling count appears in that preimage
([`EEZ.sol:709-718`](../../../sync-rollups-protocol/src/EEZ.sol#L709)). The final
per-proof-system hash only adds the rollup/vkey accumulator
([`EEZ.sol:720-743`](../../../sync-rollups-protocol/src/EEZ.sol#L720)).

The verifier API receives only `(proof, publicInputsHash)`
([`IProofSystem.sol:7-15`](../../../sync-rollups-protocol/src/interfaces/IProofSystem.sol#L7)).
The deployed ECDSA verifier recovers the signer from that digest directly
([`ECDSAProofSystem.sol`](../../../contracts/src/ECDSAProofSystem.sol)).
There is no later step that rebinds either count to the proof.

### 3. The omitted fields change dispatch and availability

After proof verification:

1. `immediateEntryCount` bounds the leading inline L2Tx loop
   ([`EEZ.sol:397-421`](../../../sync-rollups-protocol/src/EEZ.sol#L397)).
2. If a non-L2Tx entry remains inside that prefix and the poster has code, the
   remainder of the prefix and the selected static prefix are loaded into
   transient tables for the poster's meta hook
   ([`EEZ.sol:423-434`](../../../sync-rollups-protocol/src/EEZ.sol#L423)).
3. Persistent queues start exactly at `immediateEntryCount` and
   `immediateStaticEntryCount`
   ([`EEZ.sol:764-778`](../../../sync-rollups-protocol/src/EEZ.sol#L764)).
4. Any unconsumed transient values are deleted
   ([`EEZ.sol:443-448`](../../../sync-rollups-protocol/src/EEZ.sol#L443)).

Consequently, these fields do more than select timing. Depending on the poster
and batch shape, they can decide whether a proved entry remains available at
all.

## Reproduced execution-entry scenario

Consider two proved entries for one rollup:

| Index | Entry | Canonical scheduling |
| --- | --- | --- |
| `0` | Leading L2Tx/anchor, `proxyEntryHash == 0` | Immediate |
| `1` | Cross-chain entry, `proxyEntryHash != 0` | Persistent queue |

The canonical batch has `immediateEntryCount = 1`. A poster changes only that
field to `2` and submits from an EOA:

1. The changed value remains within `entries.length`.
2. The leading-L2Tx boundary guard passes: the value was increased, and there
   is no entry at index `2` to strand.
3. The public-input hash is unchanged, so the same proof remains valid.
4. Entry `0` executes and advances the loop cursor to `1`.
5. Entry `1` stops the leading L2Tx loop because its hash is nonzero.
6. The meta hook does not run because the EOA has no code.
7. Persistent publication starts at index `2`, so entry `1` is not queued.

The proved content was not changed, but entry `1` is no longer available for
consumption.

The adversarial test
[`testImmediateEntryCountCanDropADeferredEntryWithoutChangingPublicInput`](../../../contracts/test/PublicInputsHashVectors.t.sol)
constructs the canonical and mutated batches on separate `EEZ` instances and
checks that:

- their full ABI encodings differ;
- their shared and final public-input hashes are identical;
- a strict mock verifier accepts both against the same pinned hash;
- the canonical submission leaves queue length `1`;
- the mutated submission leaves queue length `0`; and
- both instances reach the same anchor state root.

The mock is non-vacuous: `setExpectedPublicInputsHash` enables exact equality
checking, rather than its default accept-all behavior
([`MockProofSystem.sol:21-29`](../../../sync-rollups-protocol/test/mocks/MockProofSystem.sol#L21)).

The test passes with:

```sh
cd contracts
forge test \
  --match-contract PublicInputsHashVectorsTest \
  --match-test testImmediateEntryCountCanDropADeferredEntryWithoutChangingPublicInput \
  -vvvv
```

The trace supplies the same public-input hash to both verifier calls:

```text
0xcc801255f4145cc6d3510cd5ea9018b19c573c541958b0fe31aa108ba796666a
```

## Why the existing guards do not remove the risk

| Guard | What it guarantees | What it does not guarantee |
| --- | --- | --- |
| Count bounds | Counts cannot exceed their corresponding arrays | A valid in-range count is proof-authorized |
| `ImmediateCountStrandsLeadingL2Tx` | A poster cannot under-count and queue a leading zero-hash L2Tx | A poster cannot over-count a later nonzero-hash entry |
| Full entry hashes | Entry content cannot be replaced without invalidating the proof | The entry remains executable or queued |
| `StateUpdate.currentState` | An executed entry must apply to the live pre-state | Every proved entry is eventually attempted |
| Rolling-hash and ether checks | Executed effects must match their proved effect chain and value flow | Omitted effects must be dispatched |
| Sender binding | With binding enabled, a different address cannot reuse the proof | The authorized poster cannot change an unbound count |

These backstops are meaningful. The reproduced behavior does **not** show a
way to forge an entry, bypass state-root checks, invent an ether delta, or apply
an arbitrary state transition. The confirmed impact is selective omission,
changed execution timing/path, changed queue availability, and possible loss
of intended cross-entry atomicity.

## Reachability in the current supported profile

`postAndVerifyBatch` is permissionless; proofs provide authorization, and
sender binding is optional
([`CORE_PROTOCOL_SPEC.md:1374-1384`](../../../sync-rollups-protocol/docs/CORE_PROTOCOL_SPEC.md#L1374)).

The current Composer sets `bindMsgSenderInPublicInput = false`
([`composer.rs:2227-2234`](../../eez-composer/src/composer.rs#L2227)), and the
proof signer currently rejects batches that set it to `true`
([`post_batch.rs:253-256`](../src/settlement/post_batch.rs#L253)). Therefore, a
different EOA can submit a count-mutated copy of a publicly observed valid
batch in the supported profile. A private submission path may reduce that
opportunity but does not create a cryptographic guarantee.

Enabling sender binding would prevent a different address from front-running
the proof. It would not bind the scheduling fields themselves, and therefore
would not protect against the authorized poster or a poster contract whose
submission path permits another caller to choose the counts.

## Static-entry counterpart

`immediateStaticEntryCount` is omitted from the same hash and is used as the
start index for persistent static-entry publication. The code therefore has
the analogous availability property.

There is an additional edge shape: the structural checks allow a nonzero
static count whenever `immediateEntryCount` is nonzero. If the entire immediate
entry prefix is consumed by the leading zero-hash loop, the meta-hook condition
`i < immediateEntryCount` is false. The selected static prefix is then neither
loaded transiently nor persisted, because static persistence starts after
`immediateStaticEntryCount`.

Current tests cover static-count bounds and successful transient static-entry
use, but there is no strict canonical-versus-mutated public-input test
equivalent to the execution-entry reproduction. The current proof-signer
profile also rejects nonempty `staticEntries`, so this counterpart is a general
protocol concern rather than an exploit through the presently admitted signer
profile.

## Current specification intentionally permits the behavior

The omission is consistent across normative and explanatory sources:

- The Solidity interface calls both values unproven and re-tunable
  ([`IEEZ.sol:30-31`](../../../sync-rollups-protocol/src/interfaces/IEEZ.sol#L30)).
- The core protocol specification repeats that definition and shows a hash
  formula without the counts
  ([`CORE_PROTOCOL_SPEC.md:319-355`](../../../sync-rollups-protocol/docs/CORE_PROTOCOL_SPEC.md#L319)).
- The multi-prover specification explicitly excludes both values
  ([`MULTI_PROVER_SPEC.md:122-159`](../../../sync-rollups-protocol/docs/MULTI_PROVER_SPEC.md#L122)).
- The execution-entry and lookup specifications describe the same unproven
  split
  ([`EXECUTION_ENTRY_SPEC.md:273-289`](../../../sync-rollups-protocol/docs/EXECUTION_ENTRY_SPEC.md#L273),
  [`STATIC_ENTRY.md:264-269`](../../../sync-rollups-protocol/docs/STATIC_ENTRY.md#L264)).
- The security section calls the meta hook untrusted and allows it to consume
  partially or ignore the call
  ([`CORE_PROTOCOL_SPEC.md:1366-1372`](../../../sync-rollups-protocol/docs/CORE_PROTOCOL_SPEC.md#L1366)).
- The same document classifies discarding unconsumed-but-proved entries as a
  liveness choice, not a safety violation
  ([`CORE_PROTOCOL_SPEC.md:1334-1338`](../../../sync-rollups-protocol/docs/CORE_PROTOCOL_SPEC.md#L1334)).

Repository history reinforces that this was considered rather than merely
overlooked. Commit `7c5db1b5` removed a comment that explicitly described an
EOA poster dropping the middle immediate prefix, while adding the current
under-count protection. It did not add an over-count or EOA fallback guard.

For that reason, it would be inaccurate to present this as a contract defect
against the current specification. The question is whether the specified
trust and liveness model is acceptable.

## Proof-signer admission boundary

The proof signer deliberately accepts only one scheduling profile:

- `staticEntries` and `immediateStaticEntryCount` must be empty; and
- `immediateEntryCount` must equal the complete leading zero-hash run.

This is an admission policy, not a claim that either count is signed. It keeps
signer inputs deterministic and is specified in [the signer scheduling
boundary](../SPEC.md#12-scheduling-boundary), but it does not close the on-chain
mutation path: a signature over an accepted batch can still be attached to a
copy with different counts because those carriers are absent from the digest.

## Impact under the accepted model

The demonstrated impact is integrity and availability of dispatch, rather than
state-transition soundness:

- a valid proof can authorize the same content while a poster changes which
  effects remain available;
- a selected cross-chain effect can be omitted without forging its content;
- advancing an anchor state can make a later replay of the canonical batch
  fail its original pre-state checks, so recovery may require a newly composed
  and attested batch;
- applications that assume all proved effects remain dispatchable can observe
  broken liveness or atomicity.

A severity rating is not assigned because the behavior matches the confirmed
protocol guarantee. Deployments must nevertheless avoid claiming that an
attestation guarantees execution, queue persistence, or an immediate/deferred
partition: none of those properties is signed.

## Protocol-owner resolution

The protocol owners resolved the open questions as follows:

1. A proof authorizes entry correctness, not continued availability or
   eventual execution.
2. The protocol guarantees immediate handling only for the leading entries
   that execute L2 transactions. Scheduling of the remaining entries is under
   poster/composer control.
3. A poster normally uses a meta hook to consume the selected immediate
   remainder. If it does not, the protocol permits those entries to remain
   unexecuted.
4. `bindMsgSenderInPublicInput` is available when a deployment needs to pin
   the permitted submitter and prevent a different address from front-running
   the batch.

Sender binding does **not** commit either scheduling count and does not compel
the bound sender to execute every entry. A network that requires stronger
execution or availability guarantees must both control/trust that sender and
define an enforceable submission policy; enabling the boolean alone is not an
execution guarantee.

The proof signer may enforce a canonical count shape as an admission policy,
but its signature cannot attest that the same counts will be used on L1. Its
specification and operator documentation must state that boundary directly.

If a future protocol profile decides to prove scheduling, protocol-level
options include:

- include both counts in the public-input preimage;
- derive the scheduling split deterministically instead of accepting mutable
  calldata fields; or
- retain flexible scheduling but persist any prefix item that was not consumed
  transiently.

Each option changes protocol semantics or the public-input ABI and therefore
requires a new protocol decision. This assessment implements none of them.
