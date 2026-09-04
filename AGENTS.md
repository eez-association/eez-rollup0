# Working in eez-rollup0

## Evidence and ownership

- Trace the current runtime path, every production caller, and the relevant tests before changing behavior; names, comments, PR descriptions, and design docs may be stale.
- Treat `eez-core-protocol` as an upstream contract, not a change surface for this repo. Integrate approved revisions by adapting downstream consumers and independent compatibility tests.
- For PR work, pin the exact base and head. If the base is stale, inspect both the historical `base...head` change and its interaction with current `main`; green historical CI is not integration evidence.
- Place shared behavior with the component that owns its full lifecycle. Keep roles and required dependencies explicit; add an abstraction or public API only for a real caller or runtime alternative.
- For failures crossing a component boundary that can trigger retries or state mutation, decide retryability independently from attribution. Bind actionable failure data to the exact request before acting, and never retry a terminal failure indefinitely.
- On transaction recovery paths, account for every item as consumed, retained, requeued, or evicted; preserve canonical ordering and nonce dependencies.
- Unsupported configurations and shapes fail explicitly. Do not use silent fallback, truncation, partial success, or ignored results where they can hide divergence.

## Code and tests

- Comments and docs describe current behavior and rationale, not the change from an earlier implementation; omit narration that restates code.
- Name operational and protocol limits and justify them from a protocol constant, upstream cap, or measured budget.
- Do not add `unwrap` or `expect` on runtime data in production paths unless the invariant is local and mechanically obvious; otherwise return an explicit error.
- Tests must exercise the production path and fail if the guarded behavior is removed. Use cross-process tests for process boundaries rather than replacing the boundary with a mock.
- Rejection and security tests prove the request reached the intended component, then assert the specific error, receipt, revert selector, ordering, or state transition. Absence of success, unchanged state, logs, or timeouts alone are insufficient.
- Mocks must not claim success for work they skip. Keep compatibility oracles independent across language or component boundaries.
- Reproduce bugs from a clean baseline at the requested revision, then rerun the same evidence after the patch. A mock, another agent's verdict, or an environment denial is not a live reproduction.

## Validation and delivery

- Before protocol or end-to-end work, verify the submodule pin and working tree; never update a modified submodule. Derive affected checks from the current CI workflow rather than a hardcoded command list.
- Run process-spawning integration tests serially unless the harness guarantees isolation; report a retry-only pass as a flake.
- Use isolated data directories for devnets and spawned processes. Stop them and remove test data afterward unless asked to retain them; keep only evidence needed for the report.
- Report exact validation and relevant checks not run. Distinguish mocked, embedded, cross-process, and live evidence; green CI does not cover untested failure paths.
- PR descriptions state the actual behavior change, motivating invariant, material tradeoffs, exact validation, and expensive lanes left to CI.
- Preserve meaningful commits during integration. Use `git range-diff` before publishing rewritten history and force-push only with `--force-with-lease=<ref>:<expected-sha>`.
