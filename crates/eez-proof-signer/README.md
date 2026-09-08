# eez-proof-signer

`eez-proof-signer` is the EEZ validation and attestation daemon. It accepts one
composer-streamed L2 block window, re-executes every block with Stateless,
binds the supported settlement effects and DA payload to that execution, and
signs the independently recomputed public-input hash.

- [`SPEC.md`](SPEC.md) is the normative protocol and compatibility contract.
- [`../eez-control-rpc/SPEC.md`](../eez-control-rpc/SPEC.md) specifies how a
  Composer constructs prover requests and uses their responses for L1 submission.
- [`docs/README.md`](docs/README.md) is the entry point for architecture,
  validation evidence, settlement, lifecycle, and operations explainers.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) records contributor constraints and verification
  commands.

The current production backend is in-process Stateless. The service is
single-flight and fails closed: any admission, validation, settlement,
deadline, or signing failure returns no signature.
