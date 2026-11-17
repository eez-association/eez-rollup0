# evm2 execution

The node runs reth v2.11.0, whose EVM is evm2. The proof signer keeps revm 43
in eez-association/provers (the `provers/` submodule, see `docs/provers.md`)
and re-executes every window, so the two EVMs check each other. Both replay the
golden fixtures in `provers/stateless-ecdsa/testing/evm-golden/` and must match
them exactly.

## The EEZ EVM (`crates/eez-evm`)

- `EezEvmTypes` is the evm2 type family. Its transaction is `EezEvmTx`:
  `Ethereum(TxEnvelope)` or `System(SystemTransaction)` for type `0x76`. Each
  EVM carries the genesis `feeCollector`.
- Ethereum transactions run evm2's handlers with `EezTxHooks`, which credit
  `min(base fee, gas price) × gas used` to the fee collector without warming
  it. Type `0x76` runs the native handler (`evm.rs`, `mod native`); see
  `docs/native-system-transactions.md`.
- The block executor is reth's `EthBlockExecutor` with its block hooks pinned to
  the Merge: no beacon roots, withdrawals or requests, and a nonzero parent
  beacon root is rejected. EIP-2935 history runs by hand from Prague.
  Transactions follow the block's own fork.

## Simulation in the composer (`crates/eez-composer`)

- `SimState` (`local/sim.rs`) is a state provider plus an evm2 `Cache` of every
  read and accepted write. Each transaction runs on a fresh EVM whose accepted
  overlay is that cache, then detaches; the caller decides what to commit. An
  evm2 `Evm` is not `Send`, so sessions keep the cache, not an EVM.
- Synthetic frames are unsigned legacy transactions with a zero gas price.
  `disable_checks` removes the base-fee, balance, nonce, EIP-3607 and block
  gas-limit checks and lifts the transaction gas caps; L1 manager frames keep
  the balance check. The sender's nonce bump is undone before commit.
- `SyncBlockState::open` takes the block builder's cache
  (`take_state_cache`), so appended transactions continue from mid-block.
- Inspectors observe calls only (`InspectorConfig::without_steps`). The proxy
  lookup reads storage without loading it, so it does not change the inspected
  transaction's gas. A call the inspector answered but the EVM then discarded
  (a top-level frame that halts before running) voids its dispatched calls like
  a revert.
- Re-entry (`overlay.rs`): before a dispatch the inspector publishes its live
  state (accepted cache plus the transaction's in-flight values). A session that
  re-enters the rollup opens over it and publishes the writes of each
  execution. The suspended inspector replays the writes published during its
  dispatch through its journal; created or destroyed accounts are rejected.

## Stateful backend checkpoints

A mid-block checkpoint roots `executor.block_state().clone().into_bundle()`.
`BlockState: Clone` comes from alloy-rs/evm2#1078. Until a reth release carries
it, `[patch]` points evm2 at `debjit-bw/evm2@5885d6b`: reth v2.11's evm2
revision `66687c1` plus that commit.

## Build

- reth's default `jit` feature, which reth_gnosis enables, links evm2-jit and
  needs LLVM 23 (`scripts/ci/install-llvm.sh`). Nothing turns JIT on at runtime.
- `deny.toml` bans revm and alloy-evm in the node workspace. The root
  `Cargo.lock` still lists revm as an optional dependency of `eez-primitives`,
  which only the signer in provers enables.

## Fork limits

The Merge pin covers block hooks only. The signer's revm 43 cannot verify
Amsterdam, so it must move to a newer EVM before Amsterdam activates on L2.
