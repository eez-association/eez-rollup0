# Native system transactions

EEZ L2 reserves EIP-2718 type `0x76`, encoded as
`0x76 || rlp([chainId, nonce, gasPrice, gasLimit, to, value, input])`.
There is no signature or caller field. The sender is always
`0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076`; the target must be EEZL2 at
`0x4200000000000000000000000000000000000007`. The transaction hash is the
Keccak-256 hash of these bytes. Trie entries and receipts retain type `0x76`.
Ethereum transaction bytes and signature recovery are unchanged.

Native execution retains ordinary nonce, chain ID, gas, fee, balance,
value-transfer, and revert rules. The system account remains funded: this change
does not introduce minting or fee exemptions. The EVM uses Ethereum's legacy fee
model; the consensus envelope and receipt carry the native type. Reverts consume
gas and advance the nonce. RPC returns `type: 0x76`, the fixed sender, ordinary
quantity fields, and no signature fields.

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

This implementation targets a **fresh genesis**. Regenerate genesis with
`scripts/update-eezl2-genesis.sh` and redeploy the L1 commitment. EEZL2's immutable
system address must match the funded, codeless reserved account. Existing L2
databases must be rebuilt because transaction storage encoding also changes.
There is no in-place activation/migration of a signed-system deployment here.
`EEZ_L2_SYSTEM_KEY` is no longer used. The proof signer's attestation key and the
Composer's L1 posting key retain their separate roles.

L1 consensus stays upstream Ethereum/Gnosis. Local cross-chain simulation shares
Ethereum execution rules through the EEZ adapter without enabling native L1
transactions. Reth's assembler, consensus and Engine validation are reused; its
payload-selection loop is adapted because the pinned entry point fixes Ethereum
primitives. `vendor/stateless` adds generic recovered-block/receipt entry points
to the pinned library while retaining witness, consensus, trie, and checkpoint
algorithms. See its provenance note for replacing it with an upstream revision.

## Live regression

`cargo test -p eez-node-e2e --test native_system -- --nocapture` launches two
Composer processes with separate L1/L2 databases, a stateless proof signer, and
two followers with public configuration only. One follower joins after both
native transaction directions have settled. The test compares every block up to
the outbound transaction's safe height, including transaction and receipt roots,
checks native receipt equality, and rejects raw native submissions at the public
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
