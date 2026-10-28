# Protocol-neutral Stateless fixture

This directory contains one block, its execution witness, and the chain rules
needed by the Stateless adapter tests. The tests use it to exercise block RLP
decoding, witness-backed execution, state-root validation, and adapter error
handling; none of those checks depends on the EEZ settlement ABI.

`block-13.rlp` contains the decoded binary RLP bytes directly, not hexadecimal
text. Its SHA-256 is
`453bed17bc61adf760335855eb93c727049fa63143d426c466e3bbf45adc31d7`.
The witness and chain configuration hashes are, respectively,
`5dc43bf664d222f333e72e211271adca779eee546a3e53c43af2f54ea884c9e1` and
`324ea9532f8d6fe23f47d2aad7fb7063e97e8d2b2e6442b9f2ea1f067a74010a`.

## Native finalization witness extension

The native `0x45` executor reads SYSTEM (`0xffff…fffe`) and EEZL2 at block
finalization. These historical captures predate those reads. The witness `state`
arrays now include additional Merkle proof nodes recovered from committed genesis
state. Each added node is authenticated by its Keccak hash on the path from the
original captured parent state root; no account value, block, root, transaction,
or proof-response oracle was changed. The source checksums above describe the
original capture, before this proof-only extension.

- `witness-13.json`: added `0x0f36abc33943d73d6715e837855aae655e45d96dbea5d57eba109e803be8357e`, `0x8b55b7742ed825a92f0106087753bea6dd93cf29a49c0810275dd5bbaff27a30`.
