# Native system transactions

EEZ L2 reserves EIP-2718 type `0x76`, encoded as
`0x76 || rlp([chainId, nonce, to, value, input])`.
There is no signature or caller field. The sender is always
`0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076`; the target must be EEZL2 at
`0x4200000000000000000000000000000000000007`. The transaction hash is the
Keccak-256 hash of these bytes. Trie entries and receipts retain type `0x76`.
Ethereum transaction bytes and signature recovery are unchanged.

L2 does not support blob transactions. The pool rejects EIP-4844, the Live
payload builder skips blobs without storing or fetching sidecars, and block/Engine
replay rejects them before execution, including stateless proof validation.
Engine API blob bundles remain present but empty. L1 still supports blobs.

Native wire transactions have no fee or gas-limit fields. Their execution environment
preserves type `0x76`, which revm recognizes as Custom, and supplies a zero gas
price. This skips Ethereum-specific fee validation while ordinary fee accounting
charges zero even with a positive block base fee. Nonce and chain-ID validation
remain enabled. The implicit protocol budget is `SYSTEM_TX_GAS_LIMIT` (2,000,000);
execution is metered and actual gas usage counts toward receipts and the block
limit. The full budget must fit the remaining block gas before execution.

Zero gas price is also visible to contracts: `tx.gasprice` (`GASPRICE`) returns
zero throughout an inbound native transaction, including nested calls through
EEZL2 and application proxies. It is not replaced by the block base fee or the
gas price of the originating L1 transaction. A reimbursement calculated as
`gasUsed * tx.gasprice` is therefore zero, and a target that requires a positive
gas price can revert. Applications depending on those assumptions need to
support fee-free inbound execution explicitly; arbitrary application behavior
is not guaranteed to match a paid transaction. Ordinary signed L2 user
transactions retain their normal gas-price and fee semantics, including the
user transaction in an outbound `[load, user]` pair.

The Composer probes inbound targets using the same native delivery path and
rejects reverting targets before accepting the composition. The EVM regression
`native_gasprice_is_zero_in_nested_calls_including_reimbursement_and_positive_price_guards`
executes small contracts with a positive block base fee, checking direct and
nested `GASPRICE`, reimbursement arithmetic, and a positive-price guard for both
native and paid transactions. These are representative behavior tests with
stand-in contracts, not a compatibility audit of third-party applications.

An inbound transaction mints exactly its `value` before the ordinary EVM call,
independently of any existing system balance. The shared `eez-evm` wrapper journals
this credit, delegates execution to Ethereum's EVM, and removes the credit if the
outer call reverts or halts. Such failures still advance the nonce and consume
gas; invalid transactions commit nothing. Nested call failures retain the normal
contract/EVM semantics. Unlike OP deposits, an outer failure does not retain the
mint. No separate `mint` field is needed because this protocol mints the attached
inbound value. Canonical reconstruction and proof validation bind that value to
the authorized cross-chain entry.

The system account has no genesis allocation, code, or initial nonce. Its first
transaction starts at nonce zero and creates it naturally. Outbound ETH continues
to accumulate at this address; subsequent deposits mint fresh value and do not
spend that existing balance. RPC retains native receipt types, gas usage and a
zero effective gas price. Transaction JSON exposes the fixed `gas` budget,
`gasPrice: "0x0"`, and zero `v/r/s` placeholders for Ethereum tooling such as
Blockscout. These compatibility fields are absent from the wire transaction and
do not affect its hash or provide signature authorization.

The type identifies a privilege domain; derivation establishes authority.
Public pooling/gossip use Ethereum's pooled envelope, which has no conversion
from native transactions. Reorg reinjection uses the same fallible conversion.
The pool additionally rejects the reserved sender. Authenticated Engine API
imports remain trusted; unsafe-head propagation is provisional until L1-derived
confirmation.

Composer, Deriver, and proof signer share `eez-protocol::system_tx` to reconstruct
identical native bytes from canonical entries and the parent-state nonce.
Outbound pairs remain `[load, user]`; inbound deliveries are standalone system
transactions. Partial Sync resumption retains consumed-prefix nonce accounting.
Intermediate Live blocks cannot contain native transactions. Normal signed
transactions calling EEZL2 do not gain native status.

The proof signer replays actual typed transactions/receipts, derives transaction
checkpoints, verifies effects, and compares omitted system transactions with the
independently reconstructed sequence byte for byte.

Inbound reconstruction uses `eez_protocol::entries::InboundSidecar`, distinct
from L1 settlement entries. Its checked constructor derives the L2 call and
rolling hashes from `IncomingEntry`. The internal entry representation retains
`ExecutionEntrySol`: one incoming call occupies `l2ToL1Calls`, state updates and
expected calls are empty, and the hashes follow the L2 rules. Explicit conversion
checks the complete shape and hashes before lowering it to
`L2ExecutionEntrySol.incomingCalls` and a native transaction. L1 settlement
entries cannot pass as inbound sidecars.

The DA container in `batch.callData` publishes `Action` values with explicit
source and destination rollup IDs, not ABI-encoded entries. The Composer projects
entries into actions; the Deriver reconstructs entries and checks them through
`InboundSidecar` before lowering incoming transactions. The typed boundary
preserves the existing action encoding, entry hashes, and native transaction
bytes.

Shape validation does not authorize an inbound call. Before attesting, the
shared proof-signer settlement pipeline inspects the executed native calldata
and successful receipt, requires source rollup 0, and recomputes the call hash
using its configured destination rollup. It binds the observation to the
settlement entry's call hash, return data, state transition, and ETH delta, then
reconstructs the entry from each DA action and compares its ABI bytes with the
independently derived sidecar. The container must also identify the configured
rollup. Finally it reconstructs the entire Sync transaction sequence and compares
the exact executed bytes. A well-formed sidecar for another source or destination
therefore still fails authorization/DA binding. Deriver reconstruction alone
is not an attestation check; it operates on L1-authenticated batches.

`inbound_sidecar_preserves_entry_encoding_and_rejects_other_entry_shapes` checks
the entry encoding and rejects altered shapes and identity fields. The signer
regression `a_fully_bound_inbound_passes_settlement_and_da_validation` accepts
the valid control, then rejects raw L1 settlement ABI substituted for DA and
valid DA containers whose actions have different source/destination identities.
Action hashes are recomputed during verification, so those identity mutations
do not depend on stale submitted hashes. This test uses stubbed execution
evidence and the real settlement/DA pipeline; rejection occurs before the
signing stage.

This implementation targets a **fresh genesis**. Regenerate genesis with
`scripts/update-eezl2-genesis.sh` and redeploy the L1 commitment. EEZL2's immutable
system address must match the codeless reserved address; the generator removes
its allocation from the base genesis. Existing L2
databases must be rebuilt because transaction storage encoding also changes.
There is no in-place activation/migration of a signed-system deployment here.
`EEZ_L2_SYSTEM_KEY` is no longer used. The proof signer's attestation key and the
Composer's L1 posting key retain their separate roles.

L1 consensus stays upstream Ethereum/Gnosis. Local cross-chain simulation shares
Ethereum execution rules through the EEZ adapter without enabling native L1
transactions. Reth's assembler, consensus and Engine validation are reused; its
payload-selection loop is adapted because the pinned entry point fixes Ethereum
primitives. Stateless provides generic recovered-block/receipt entry points while
retaining witness, consensus, trie, and checkpoint algorithms. Until the change
lands via [EEZ Stateless PR #2](https://github.com/eez-association/stateless/pull/2),
the dependency uses `AdityaSripal/stateless` branch
`aditya/generic-recovered-validation`, pinned to an exact commit in `Cargo.lock`.

## Live regression

`cargo test -p eez-node-e2e --test native_system -- --nocapture` launches two
Composer processes with separate L1/L2 databases, a stateless proof signer, and
two followers with public configuration only. One follower joins after both
native transaction directions have settled. The test compares every block up to
the outbound transaction's safe height, including transaction and receipt roots,
checks a real ETH deposit and withdrawal, native nonces, zero fees, receipt
equality, and rejects raw native submissions at the public
RPC decoder. Successful output includes the exact native transactions, receipts,
and containing block roots.

The development L1 auto-mines, and an Engine API feed imports its blocks into the
second Composer's independent L1. The embedded relay does not provide atomic
bundle inclusion, so the test's relay selects one posting Composer per phase;
both Composers execute and derive throughout. This exercises native transactions
across real processes and real proof validation, not production PoS consensus or
competing atomic builder bundles. The existing multi-Composer/reorg tests cover
their own convergence scenarios separately.

Set `EEZ_TEST_LOG_DIR` to retain logs. Leave `EEZ_TEST_DATADIR_DIR` unset for
automatic database cleanup. Handles stop every child process on completion.
