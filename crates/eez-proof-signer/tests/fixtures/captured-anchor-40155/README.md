# Captured Chiado anchor window ending at L2 block 40155

This fixture is a complete captured signer input for L2 window
`[40146..40155]` from the Chiado deployment using protocol commit
`6fcc90b65063831cb7797e9fa361004064d28f9f`.

The block RLP and execution witnesses were read from the node's persistent
witness database. `postbatch.hex` is the exact calldata later mined in Chiado
transaction
`0xf0440f5964ca692f51389032fc07e295173f504e99c86f174d66859c2c23ee26`.
The committed calldata bytes hash to the `postbatch_calldata_sha256` recorded in
`oracle.json`; that digest covers the decoded bytes, not the stored hex text.

The signer accepted this window at `2026-08-04T09:24:01Z` and recomputed
`0x2b79886e5ca16d7896f5984a8c0d838f7ff3ec9eedba5a3dc5fd0563423d85a9`.
The transaction was mined at `2026-08-04T09:24:10Z`. The replay verifies that
the proof retained in that calldata recovers the Chiado attester.

The captured batch predates the explicit `fromBlock` field. The regression test
first checks the authentic calldata and proof, then replaces only its empty
height-unbound DA payload with the equivalent current range-bound payload. It
recomputes the public-input hash and runs the complete stateless
settlement-to-signing path against the captured blocks and witnesses. Inbound,
outbound, and mixed effects remain covered by their dedicated behavioral and
E2E tests.
