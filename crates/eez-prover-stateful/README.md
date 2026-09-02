# eez-prover-stateful

This crate is a node-backed implementation of the existing `prove.v1.Prover`
service. It is hosted by an L1-derived-only `eez-node` follower so it can read
the follower's canonical L2 state directly.

The implementation imports `eez-proof-signer`. Both backends use
the same request admission, settlement gates, public-input recomputation, and
attestation code. The comparison is therefore limited to execution evidence:

| Stateless signer | Stateful signer |
| --- | --- |
| Replays each block from its streamed witness | Replays the complete range from local state at `from_block - 1` |
| Trust input is an operator-supplied chain document | Trust input is the follower's chain spec and canonical database |
| Runs as a standalone daemon | Runs as a separately addressable gRPC service inside the follower process |

For a request covering `m..=n`, the backend:

1. returns `UNAVAILABLE` if the follower has not reached `m - 1`;
2. rejects the request if its anchor or any already-local block conflicts with
   the follower's canonical view;
3. opens historical state at `m - 1` and replays every proposed block through
   `n`, including intermediate and terminal Sync blocks;
4. checks body/header consensus, receipt commitments, transaction checkpoints,
   and every post-state root; and
5. drops the in-memory overlay after the shared settlement pipeline signs or
   rejects the request.

The canonical node database is never changed, so success and failure require no
rollback. If the anchor changes during replay, the RPC returns `ABORTED`.

## Running

Set `EEZ_STATEFUL_PROOF_SIGNER_ADDR` on a follower to enable the service. A
follower configured with `EEZ_SEQUENCER_RPC` is rejected because this first
implementation deliberately uses an L1-derived-only view.

The service uses the deployment settings shared with the stateless signer:
`EEZ_ROLLUP_ID`, `EEZ_VKEY`, `EEZ_PROOF_SYSTEM`, `EEZ_ATTESTER_ADDRESS`,
`EEZ_L2_SYSTEM_KEY`, and `EEZ_L2_SYSTEM_ADDRESS`. Its attestation secret uses a
distinct name, `EEZ_STATEFUL_PROOF_SIGNER_KEY`, so enabling it does not put the
node into Composer mode.

The current `prove.v1` stream still requires execution witnesses for wire
compatibility. They remain subject to admission quotas but this backend ignores
their contents.
