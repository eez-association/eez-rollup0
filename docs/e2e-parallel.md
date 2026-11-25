# Running the e2e suites in parallel

The default and `ci` nextest profiles run the tests of `crates/eez-node-e2e` one at a
time. The `local` profile runs several at once, for a large machine.

## The command

```bash
# The suites start target/debug/eez-proof-signer and eez-prover-stateful. The harness
# builds them only when they are missing, so rebuild them after a change:
scripts/provers-cargo.sh build -p eez-prover-stateless --bin eez-proof-signer --target-dir target
scripts/provers-cargo.sh --stateful build -p eez-prover-stateful --bin eez-prover-stateful --target-dir target

cargo nextest run -p eez-node-e2e --profile local --no-fail-fast
```

## What the profile does

- Every test of the package is in the test group `e2e-local`, which runs at most eight
  slots at once (`.config/nextest.toml`).
- A test that runs two composer nodes takes two slots: `native_system`, and in `e2e` the
  `based_mode_`, `happy_case_two_composers_`, `multi_composer_`, `two_composers_` and
  `streaming::` tests. Any other test takes one.
- nextest runs each test in its own process. A port the testkit leases stays claimed by a
  lock file under `$TMPDIR/eez-testkit-ports/` until that process exits, so two tests
  never get the same port, also while a node restarts on its old ports
  (`crates/eez-testkit/src/ports.rs`).
- Leased ports come from 10000 up to the start of the kernel's ephemeral range
  (`/proc/sys/net/ipv4/ip_local_port_range`), so no outgoing connection can take one
  between the lease and the node's bind.
- Datadirs, logs (`<name>-<pid>-<n>.log` under `EEZ_TEST_LOG_DIR`) and the signer's
  working directory are per test.

## Choosing the number of slots

Eight slots were chosen on a 20-core, 62 GB machine, where a full pass takes about 13
minutes instead of about 86 one at a time and adds about 6 GB of memory in use.

On a smaller machine, lower the number for a run with `-j`:

```bash
cargo nextest run -p eez-node-e2e --profile local --no-fail-fast -j 4
```

`-j` limits the slots of the whole run, so `-j 4` runs at most four one-slot tests, or two
two-slot tests, at a time. Without `-j`, nextest allows as many slots as the machine has
CPUs, and never more than the eight of `e2e-local`. To change the limit for good, edit
`max-threads` of `e2e-local`.

## When a test fails only under parallel load

Re-run that test alone with the default profile:

```bash
cargo nextest run -p eez-node-e2e --test <suite> -- --exact <test>
```

If it passes alone twice, it is load-sensitive: read its node logs (set `EEZ_TEST_LOG_DIR`
to keep them) to find the race, and fix it in the test or the node. Do not add retries to
hide it.
