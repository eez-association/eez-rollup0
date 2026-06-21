# Fuzz Testing

The rationale lives next to the code now (so it stays in sync). Start here:

- **Oracle design** — what makes a `Composition` correct, what each assertion
  catches, and what's out of scope → module docs in
  [`crates/eez-fuzz/src/assertions.rs`](../crates/eez-fuzz/src/assertions.rs).
- **Why fuzz the composer + how it's made fuzzable** → the cargo-fuzz target
  [`crates/eez-fuzz/fuzz/fuzz_targets/compose.rs`](../crates/eez-fuzz/fuzz/fuzz_targets/compose.rs).
- **Structure-aware input generator** (the address-space dictionary) →
  [`crates/eez-fuzz/src/generator.rs`](../crates/eez-fuzz/src/generator.rs).
- **World boot + fixture invariants** →
  [`crates/eez-fuzz/src/lib.rs`](../crates/eez-fuzz/src/lib.rs).
