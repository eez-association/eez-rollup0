# eez-prover-stateful

This crate is a node-backed implementation of the `prove.v1.Prover/ProveStream`
service. It is hosted by the L1-derived-only `eez-follower` process so it can read
the follower's canonical L2 state directly.

The implementation imports `eez-proof-signer`. Both backends use
the same request admission, settlement gates, public-input recomputation, and
attestation code. The comparison is therefore limited to execution evidence:

| Stateless signer | Stateful signer |
| --- | --- |
| Replays each block from its streamed witness | Imports blocks through Reth and reuses local execution evidence |
| Trust input is an operator-supplied chain document | Trust input is the follower's chain spec and canonical database |
| Runs as a standalone daemon | Runs as a separately addressable gRPC service inside the follower process |

`Begin` checks the exact L1-derived safe anchor. `Block` imports candidate
execution through the follower's Reth engine and binds its retained block,
receipts and parent state. Sync checkpoints are replayed and cached when
needed; ordinary blocks reuse normal engine execution.

`Finalize` selects the checked terminal under the follower's reconciliation
lock. Reth enforces safe/finalized ancestry, and a rejected or unavailable
selection cannot produce a proof. Imported execution stays in Reth; sessions
retain only references and supplemental evidence.

## Running

Set `EEZ_STATEFUL_PROOF_SIGNER_ADDR` on a follower to enable the service. A
follower configured with `EEZ_SEQUENCER_RPC` is rejected because this first
implementation deliberately uses an L1-derived-only view.

The service uses the deployment settings shared with the stateless signer:
`EEZ_ROLLUP_ID`, `EEZ_VKEY`, `EEZ_PROOF_SYSTEM`, `EEZ_ATTESTER_ADDRESS`,
and `EEZ_L2_SYSTEM_ADDRESS` (the reserved native system address). Its attestation
secret uses a distinct name, `EEZ_STATEFUL_PROOF_SIGNER_KEY`, so enabling it does not put the
node into Composer mode.

The current `prove.v1` stream requires execution witnesses under the shared wire
contract. They remain subject to admission quotas but this backend ignores
their contents.
