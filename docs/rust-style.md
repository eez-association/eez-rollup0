# Rust style guide

This guide defines the default Rust style for `eez-rollup0`, with particular
focus on `eez-deriver`, `eez-driver`, `eez-l1`, and `eez-node`. It applies to
new code and to code materially changed by a pull request. Existing code that
does not yet follow the guide is not a reason for unrelated cleanup.

The goals are, in order:

1. preserve protocol and operational correctness;
2. make authority and data provenance visible;
3. keep state transitions and failure handling locally understandable;
4. minimize the amount of code and number of concepts needed;
5. make operational behavior observable without noisy or unsafe logs.

The words **must**, **should**, and **may** are intentional. A **must** is a
review requirement. A **should** is the normal choice; deviations need a
concrete reason. A **may** is optional.

## Authority

When sources disagree, use this order:

1. pinned contracts and normative protocol specifications;
2. Rust's type and memory-safety rules;
3. workspace compiler, rustfmt, Clippy, and CI configuration;
4. this guide;
5. crate- or module-specific documentation.

Correctness takes priority over style. Resolve a contract/specification gap
explicitly; do not silently weaken validation or encode an assumption merely
to make code or tests pass. Crate-specific contributor guides may impose
stricter requirements.

## Crate responsibilities

Keep responsibilities at their current architectural boundary:

| Crate | Owns | Must not own |
|---|---|---|
| `eez-node` | process configuration, mode selection, dependency wiring, component lifetime, application adapters | protocol hashing, settlement rules, L1 scanning algorithms, or L2 state-transition logic |
| `eez-l1` | canonical L1 observation, log and calldata scanning, reorg handling, transaction/bundle submission | L2 replay, block production, or application environment parsing outside a dedicated config boundary |
| `eez-driver` | L2 timing, block production, Engine API interaction, block commitment, actor state | contract calldata interpretation, L1 submission, or node bootstrap policy |
| `eez-deriver` | converting canonical L1 batch records and DA into deterministic L2 replay and safe-head advancement | environment parsing, L1 transaction submission, or duplicated protocol algorithms |

Shared contract types, encodings, hashes, and system-transaction construction
belong in `eez-protocol`. A consumer must call the shared implementation rather
than reproduce it locally.

Dependencies should point toward lower-level capabilities:

```text
eez-node
  ├──> eez-deriver ──> eez-driver ──> eez-l1 ──> eez-protocol
  │         ├──────────────> eez-l1
  │         └──────────────> eez-protocol
  ├──> eez-driver
  ├──> eez-l1
  └──> eez-protocol
```

Do not introduce a reverse dependency to avoid defining a small interface or
moving shared domain behavior to its actual owner.

## Change discipline

- A change should have one primary purpose. Do not mix a contract migration,
  unrelated refactor, comment rewrite, and logging redesign in one patch.
- Keep the diff against `main` minimal. Preserve surrounding code when it is
  still correct and understandable.
- Update an adjacent name or comment when the changed behavior would otherwise
  make it false. Do not reword unaffected code for taste.
- Prefer deleting duplicated branches and checks over introducing another
  abstraction around them.
- Do not copy a defensive check into every layer. The owning layer validates it
  once and returns a type or value that records the guarantee.
- Generated files and pinned external code are changed only as part of an
  explicit upgrade.
- A broad cleanup should be its own reviewed change with behavior-preserving
  tests.

## Formatting and linting

The workspace uses Rust 2024 and Rust 1.93. Stable rustfmt owns whitespace,
line wrapping, and basic layout; do not hand-format around it.

All code must pass the workspace lint configuration. The Clippy allow-list in
`Cargo.toml` exists for compatibility and integration constraints. An allowed
lint such as `too_many_lines`, `too_many_arguments`, or `type_complexity` is
not an endorsement of hard-to-follow code.

Organize imports into these groups when the distinction is useful:

1. `core`/`std`;
2. external crates;
3. workspace and current-crate imports.

Avoid glob imports outside established preludes and test modules. Import a
trait as `_` when it is needed only for method resolution and naming it would
otherwise add noise.

## Modules and visibility

- `lib.rs` should describe the crate, declare modules, and expose its deliberate
  public API. It should contain little implementation logic.
- `main.rs` is a composition root: load typed configuration, construct
  dependencies, launch components, and await termination. Move protocol and
  subsystem behavior to the owning crate or module.
- Split modules by responsibility, state ownership, dependency set, or test
  concern—not merely because a file crossed an arbitrary line count.
- Avoid generic dumping grounds such as `utils.rs`, `helpers.rs`, or `common.rs`
  in production code. Name a module after the concept it owns.
- Items are private by default. Use `pub(crate)` for workspace-internal
  collaboration and `pub` only for the intended crate API.
- Re-export a public type from one clear location. Do not expose both an
  implementation module and all its contents without a reason.

Function and file size are review signals, not mechanical limits:

- At roughly 40–60 nontrivial lines, check whether a function still operates at
  one abstraction level.
- Above roughly 80 nontrivial lines, prefer named phase helpers unless the
  function is strictly linear orchestration.
- A source file approaching 800–1,000 lines should be reviewed for multiple
  owners or independently testable responsibilities.

A function should be split when it mixes parsing, validation, state mutation,
I/O, retries, and reporting; has several major branches; or requires phase
comments just to remain navigable. Do not add a blanket lint exemption in
place of that review.

## Naming and data provenance

A name should answer both “what is this?” and, at a trust boundary, “where did
it come from?” Use provenance consistently:

| Prefix | Meaning |
|---|---|
| `wire_` / `received_` | bytes or fields received from a peer or request, not validated |
| `claimed_` | a semantic claim made by calldata, a batch, a header, or another external source |
| `decoded_` | syntactically decoded; not necessarily trusted or semantically valid |
| `observed_` | read locally from a provider, receipt, event, or executed block |
| `configured_` | parsed operator configuration |
| `expected_` | the local value or policy against which another value is checked |
| `computed_` / `derived_` | recomputed locally from identified inputs |
| `validated_` | passed the named validation and is safe for the next stage |
| `verified_` | an externally claimed fact matched locally derived evidence |
| `canonical_` | obtained from, or proven against, the canonical chain view |
| `raw_` | encoded bytes; add the encoding when useful, such as `raw_rlp` or `raw_tx` |

Use `trusted_` only when the value has a documented root of authority. A value
is not trusted merely because it came from configuration; `expected_rollup_id`
is normally clearer than `trusted_rollup_id`.

Avoid vague names such as `data`, `state`, `result`, `item`, `value`, `info`, or
`ctx` in a scope where multiple such concepts exist. Short names are fine in a
small closure or mathematical loop when their meaning is immediate.

Do not shadow a variable when the new value has different provenance or trust
status. Make the transition explicit:

```rust
let claimed_state_root = decoded_batch.final_state_root;
let computed_state_root = replay_output.state_root;
ensure_state_root_matches(claimed_state_root, computed_state_root)?;
let validated_state_root = computed_state_root;
```

Function names should reveal their effect:

- `read_*` performs I/O;
- `parse_*` converts text or configuration;
- `decode_*` converts a wire encoding;
- `compute_*` or `recompute_*` derives a value locally;
- `validate_*` applies structural or policy rules;
- `verify_*` checks evidence, cryptographic binding, or an externally claimed
  fact;
- `ensure_*` is a guard returning `Result<(), _>`;
- `build_*` constructs without committing external state;
- `apply_*` or `commit_*` mutates authoritative state;
- `spawn_*` creates a task and must make ownership clear.

Use `try_*` when failure is an ordinary part of attempting an operation, such
as a checked state update—not as a generic prefix for every fallible function.
Predicates use `is_*`, `has_*`, `can_*`, or `should_*`.

Use Rust casing for general acronyms (`Rpc`, `Url`, `Evm`, `Tx`, `Id`) while
retaining conventional domain names such as `L1` and `L2`. Solidity-generated
field names may remain camelCase at the ABI boundary; translate them into
idiomatic domain types rather than spreading that naming through the crate.

## Types and invariants

- Make invalid states difficult to represent. Use enums for finite states and
  modes instead of related booleans.
- Use existing domain newtypes such as `RollupId`; do not pass several
  interchangeable `u64` values through an API when their meanings differ.
- Use `NonZero*`, bounded constructors, or validated wrapper types when a value
  has a persistent invariant.
- Use `Option<T>` only for genuine absence. Do not encode absence with zero,
  an empty string, a zero address, or a dummy private key in production paths.
- `Default` is appropriate only when every default field forms a safe,
  meaningful domain value.
- Constructors validate permanent invariants. Temporary workflow checks belong
  to the operation that has enough information to decide them.
- Use `new` for infallible construction and `try_new` when construction can
  reject input. Do not expose an unchecked `new` for a type whose methods rely
  on prior validation.
- Prefer a parameter struct over a long tuple or a function argument list whose
  values always travel together.
- Avoid boolean parameters at call sites when `true`/`false` does not explain
  the policy. Use a small enum such as `WitnessFeed::Emit`/`Skip`.
- `Option<T>` means expected absence; `Result<Option<T>, E>` distinguishes
  expected absence from a failed lookup or operation.
- Prefer an enum state machine with explicit transitions over multiple atomics
  or booleans that can form contradictory combinations.

Represent completed validation in the type flow:

```rust
fn validate_batch(input: DecodedBatch) -> Result<ValidatedBatch, ValidationError>;

fn derive_blocks(batch: &ValidatedBatch) -> Result<Vec<DerivedBlock>, DeriverError>;
```

Downstream code should consume `ValidatedBatch`; it should not repeat the same
checks against `DecodedBatch` “just in case.”

## Functions and control flow

- Keep one abstraction level per function. An orchestration function should
  read top-to-bottom as named phases.
- Prefer early returns and `let ... else` for rejection and absence paths.
- Use a straightforward loop when an iterator chain hides mutation, error
  context, ordering, or short-circuit behavior.
- Avoid clever compression. Fewer lines are useful only when the code remains
  obvious and preserves all checks.
- Keep side effects near the statement that authorizes them. Validation should
  precede mutation or submission.
- Use checked arithmetic at wire, configuration, index, size, block-range, and
  timeout boundaries. Do not replace it with `+` merely to shorten code.
- Use saturating arithmetic only when clamping is the intended domain behavior;
  it must not hide malformed configuration or inconsistent chain state.
- `debug_assert!` is for internal development invariants. External input,
  provider behavior, and safety requirements need a runtime error.
- Production code must not use `unwrap()` or `expect()` unless the condition is
  statically guaranteed or established immediately beforehand. An `expect`
  message must state the invariant, not repeat “should exist.”

## Protocol and trust boundaries

Treat RPC responses, logs, calldata, block payloads, DA, peers, and Composer
input as untrusted until the owning layer validates them.

At each boundary:

1. bound sizes and counts before allocating;
2. decode exactly using the canonical ABI or codec;
3. validate the rules owned by that layer;
4. preserve exact bytes where hashes or signatures bind the encoding;
5. convert into a type that records what is now known;
6. pass the validated representation downstream.

Integer widths and field order at an ABI boundary are part of correctness.
Never widen a `uint64` to `uint256` in a local interface because it is
convenient. Prefer generated/shared ABI types. A narrow local `sol!` interface
is acceptable only when a shared binding is unavailable; lock selectors or
cross-language vectors when a mismatch could silently call another function.

Addresses, chain IDs, rollup IDs, keys, verification keys, and contract code
bindings come from typed deployment configuration. Never use public development
keys or hardcoded privileged addresses as a production default.

## Errors

Library crates use a crate-specific typed error and `Result` alias. Two forms
are acceptable:

- a `thiserror` enum when variants are part of the public API; or
- a public opaque error wrapping a private kind when API stability requires it.

Use one form consistently within a crate. `eyre` belongs at binary bootstrap,
tests, and other top-level composition boundaries—not in reusable domain APIs.

Error variants carry structured facts:

```rust
#[derive(Debug, thiserror::Error)]
enum ScanError {
    #[error("transaction {tx_hash} is unavailable in L1 block {block_number}")]
    TransactionUnavailable {
        block_number: u64,
        tx_hash: B256,
    },
}
```

Prefer this to `Rejected(String)` or a long formatted message assembled at the
failure site. Use `String` detail only when an upstream type cannot reasonably
be retained. Preserve source errors with `#[source]` where possible.

Error messages should be concise, factual, lowercase, and stable. Put dynamic
facts in fields, not several alternative prose formulations. Operational
remediation belongs in documentation or a single boundary log.

Add context once when crossing a subsystem boundary. Intermediate layers
propagate errors without logging them repeatedly. Log at the layer that chooses
retry, rejection, degradation, shutdown, or another recovery action.

When converting between crate error types, preserve the original semantic
category. Do not map an unrelated provider, timing, or channel failure to a
convenient but false variant such as `InvalidForkchoice`.

Expected outcomes are not errors. Represent bundle inclusion/drop, stale work,
or a retry decision with a result enum when callers must branch on it.

## Async code, actors, and cancellation

- Never hold a synchronous mutex or read/write guard across `.await`.
- Prefer bounded channels. An unbounded channel requires a documented and
  enforced bound on producer pressure and memory growth.
- Every long-lived spawned task has an owner, a shutdown/cancellation path, and
  an explicit failure policy.
- Use the node's critical-task executor for service actors whose exit should
  affect process health. Raw `tokio::spawn` is for bounded child work or a
  deliberately detached task whose semantics are documented.
- A loop stops on cancellation, channel closure, or owner shutdown. It must not
  silently recreate a dead dependency forever.
- Set `MissedTickBehavior` deliberately for every interval.
- CPU-heavy execution, database traversal, trie work, and blocking clients run
  under `spawn_blocking` or a dedicated worker.
- Do not scatter cancellation checks through cheap straight-line code. Check at
  meaningful boundaries between non-interruptible units.
- Timeouts have one clear owner. Avoid nesting several timeouts over the same
  operation unless each represents a distinct contract.
- Retry loops classify retryable failures, cap backoff, observe cancellation,
  and expose the attempt count at `DEBUG`.
- If atomics synchronize more than independent counters, document why their
  memory ordering is sufficient. Related state should normally share one owner
  or synchronization primitive.

Moving a value into `Arc` or cloning it should correspond to a real ownership
boundary. Use `Arc<[T]>` for immutable collections shared across tasks or
blocking work, not as a default replacement for `Vec<T>`.

## Tracing and operational output

Use structured events with stable names:

```rust
event!(
    name: "eez.deriver.batch.rejected",
    Level::WARN,
    rollup_id,
    tx_hash = %tx_hash,
    error = %error,
    "batch derivation rejected",
);
```

Field names are `snake_case` and should be reused consistently:
`block_number`, `block_hash`, `tx_hash`, `rollup_id`, `attempt`, `elapsed_ms`,
and `error`.

Choose levels by operational actionability:

- `ERROR`: an invariant failed, a critical task terminates, or manual action is
  required;
- `WARN`: external input was rejected or a recoverable anomaly deserves
  operator attention;
- `INFO`: startup/shutdown, mode selection, a major state transition, or a
  successful externally meaningful outcome;
- `DEBUG`: routine retries, per-batch/per-block progress, and decisions useful
  while diagnosing behavior;
- `TRACE`: per-transaction, per-call, and detailed RPC diagnostics.

Do not emit `INFO` on every poll iteration or for each ordinary block. Do not
log the same failure at every propagation layer. A span is useful when several
events share a request, batch, or reconciliation identity; record that identity
once on the span.

Never log private keys, raw signed transactions, full calldata, witnesses,
authorization headers, or unbounded upstream response bodies. Prefer hashes,
counts, byte lengths, and redacted/custom `Debug` implementations.

## Comments and rustdoc

Comments explain the current code, not its history.

Document:

- public API contracts;
- non-obvious safety, ordering, and state-machine invariants;
- authority and provenance at trust boundaries;
- cancellation safety and task lifetime;
- why a tempting simplification would be incorrect;
- error or panic behavior when it is not evident from the type.

Do not require a comment on every private helper. Clear names and small
functions are better than comments that restate the next line.

Avoid:

- migration stages, old implementations, incidents, branches, commits, PRs, or
  future plans;
- source line numbers in another file or contract;
- unexplained labels such as “invariant 7”;
- comments that promise checks performed by another layer without naming the
  validated type or boundary;
- stale `TODO` comments. Track future work separately and document only the
  current limitation when readers need it to use the code safely.

A useful invariant comment states the dependency:

```rust
// Read the cursor once: the anchor root and DA range must use the same L1 view.
let posted_block = l1_head.cursor();
```

Every `unsafe` block must have a nearby `SAFETY:` comment that states all
conditions making the operation sound. Keep the unsafe region as small as
possible.

## Configuration and secrets

- Parse environment variables once in a dedicated configuration boundary.
  Domain and orchestration code consume typed configuration.
- A default applies only when a variable is absent. A present but malformed
  value must fail startup.
- Validate cross-field constraints before launching any task.
- Prefer `Duration`, `Url`, `Address`, domain IDs, and non-zero integer types to
  strings and unlabelled integers.
- Use checked arithmetic for derived ports, heights, sizes, and deadlines.
- Separate read-only capability from signing/submission capability in the type
  system; do not install a dummy key to satisfy an unrelated field.
- Secret-bearing types must not derive an exposing `Debug`. Implement a
  redacted view that prints only public addresses or metadata.

## Testing

Place focused unit tests beside the module that owns the behavior. Use
`tests/` for public crate behavior and process/E2E scenarios. Split shared test
support by responsibility when a single `common` module becomes difficult to
navigate.

Test names describe behavior:

```text
condition_action_expected_result
wrong_source_chain_is_rejected_without_pool_mutation
far_behind_chunk_failure_advances_nothing
```

Tests should:

- assert state, receipts, events, exact bytes, or typed outcomes—not log text as
  the primary oracle;
- cover the failure path and prove that rejected work caused no partial
  mutation;
- use deterministic fixtures and explicit clocks where possible;
- give every async wait a deadline and a useful timeout error;
- own and clean up ports, processes, temporary directories, and chain state;
- use RAII for child processes and other external resources;
- keep cross-language vectors when Rust must be byte-identical to Solidity;
- include a regression test when fixing a bug, named for the required behavior
  rather than the incident.

Tests may use `unwrap`/`expect` for small, obvious setup. Long integration flows
should return `Result` and attach context so failures identify the phase and
relevant object.

## Ownership and performance

- Borrow by default. Take ownership when storing, transferring to a task, or
  intentionally consuming a value.
- Avoid cloning to satisfy the borrow checker without understanding the
  lifetime. Make task and state ownership explicit instead.
- Avoid allocation based on an untrusted count until the count is bounded.
- Preserve streaming behavior for potentially large block, receipt, calldata,
  and witness inputs.
- Optimize only a measured path, but remove accidental repeated decoding,
  hashing, provider calls, and collection building when ownership makes the
  duplication clear.
- Prefer one canonical representation in memory. Do not maintain parallel
  vectors that can lose alignment; group related facts in a struct.

## Review and verification

Before requesting review, run the commands that correspond to the affected
scope. The repository-wide baseline is:

```bash
cargo fmt --all --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --lib --bins --no-fail-fast
```

Run the dedicated protocol-vector, `eez-node` E2E, and cross-chain E2E suites
when those boundaries change. CI is the source of truth for the exact commands
and serial test profiles.

Reviewers should be able to answer yes to all of these:

- Is each value's meaning and authority clear from its type or name?
- Is every validation owned by one layer and represented downstream?
- Does the function read at one abstraction level?
- Are state mutation and external side effects authorized by preceding checks?
- Are errors typed, concise, and logged only where recovery is decided?
- Are task lifetime, cancellation, and backpressure explicit?
- Are logs useful at the selected level and free of secrets/unbounded payloads?
- Do comments explain current invariants without historical narration?
- Does the test assert observable behavior and the absence of partial effects?
- Is every changed line necessary for the stated purpose?
