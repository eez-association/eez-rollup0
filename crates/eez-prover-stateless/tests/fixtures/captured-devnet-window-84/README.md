# Captured signer window, L2 blocks 79–84

A complete, successful signer input recorded from a live devnet rig, and what
the service produced for it. Regenerate with:

```
bash scripts/capture-signer-window.sh <out-dir>
```

Every value is read back from the chain or the signer's own log, never
recomputed by the code under test:

- `block-<n>.rlp.hex` — consensus RLP, `debug_getRawBlock`
- `witness-<n>.json` — replay witness, `debug_executionWitness`
- `blocks.json` — hash and parent hash read from the chain, so a corrupt RLP
  cannot agree with itself
- `postbatch.hex` — the bytes mined in L1 transaction
  `0xcc601428bf01c4065d0b250565eb73dae507249a534772841638b7331a00c71b`
  (L1 block 25566), correlated to this window through the composer's own
  settlement record rather than "the most recent postBatch"
- `oracle.json` — `public_inputs_hash` is the digest the signer attested;
  `expected_test_signature` is signed independently of the service, so the
  assertion compares against a value the service did not produce

The window endpoints are **block hashes** (`window_pre_block_hash`,
`window_post_block_hash`), which is the substitution this fixture exists to
pin: under state-root commitments those fields held a different kind of value.

## Why devnet and not chiado

The previous fixture came from Chiado. Recapturing there needs a chiado
execution-layer peer that serves history; the host used for this capture could
not obtain one (see `docs/plans/BLOCK-ROOT-COMMITMENT-REBASE.md`). The chiado
artifact that *was* captured is anchor-only and lives in
`crates/eez-protocol/tests/fixtures/captured-chiado-anchor-23104703`; it pins
the public-input digest, attester recovery and the block-hash chain against a
genuinely mined chiado transaction, but carries no window.

## Native finalization witness extension

The native `0x45` executor reads SYSTEM (`0xffff…fffe`) and EEZL2 at block
finalization. These historical captures predate those reads. The witness `state`
arrays now include additional Merkle proof nodes recovered from committed genesis
state. Each added node is authenticated by its Keccak hash on the path from the
original captured parent state root; no account value, block, root, transaction,
or proof-response oracle was changed. The source checksums above describe the
original capture, before this proof-only extension.

- `witness-79.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x74c29e23aea2b198502333e222da631a276ad587027bd00628203e75f7e98325`, `0xdb6de720b307a58caf10e0dae6806fb2bb1131aa39e81ba1d2309ce18e7bc932`.
- `witness-80.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x74c29e23aea2b198502333e222da631a276ad587027bd00628203e75f7e98325`, `0xdb6de720b307a58caf10e0dae6806fb2bb1131aa39e81ba1d2309ce18e7bc932`.
- `witness-81.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x74c29e23aea2b198502333e222da631a276ad587027bd00628203e75f7e98325`, `0xdb6de720b307a58caf10e0dae6806fb2bb1131aa39e81ba1d2309ce18e7bc932`.
- `witness-82.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x74c29e23aea2b198502333e222da631a276ad587027bd00628203e75f7e98325`, `0xdb6de720b307a58caf10e0dae6806fb2bb1131aa39e81ba1d2309ce18e7bc932`.
- `witness-83.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x74c29e23aea2b198502333e222da631a276ad587027bd00628203e75f7e98325`, `0xdb6de720b307a58caf10e0dae6806fb2bb1131aa39e81ba1d2309ce18e7bc932`.
- `witness-84.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x74c29e23aea2b198502333e222da631a276ad587027bd00628203e75f7e98325`, `0xdb6de720b307a58caf10e0dae6806fb2bb1131aa39e81ba1d2309ce18e7bc932`.
