# eez-evm-inspector

EVM-specific inspection and state-overlay support for cross-chain composition.

The crate keeps the `revm`-coupled parts of composition separate from
`eez-protocol`. It detects calls to authorized cross-chain proxies while an EVM
transaction executes, dispatches those calls through the protocol's
`CompositionBuilder`, and applies nested entry-chain state changes through a
shared overlay channel.

## Exports

- `SessionInspector` detects and dispatches authorized proxy calls.
- `SessionInspectorFactory` builds inspectors with the configuration for one
  chain.
- `OverlayChannel`, `OverlayChannelHandle`, and `new_overlay_channel` coordinate
  nested execution against the entry chain's in-progress state.
- `apply_overlay_diff`, `clone_state`, and `OverlayError` provide the state
  overlay primitives.

Composition orchestration, protocol types, and errors remain in
[`eez-protocol`](../eez-protocol/); this crate does not re-export them or define
EVM-specific `Composer` or `TargetConfig` aliases.

## Dependency direction

```text
eez-protocol
     ↑
eez-evm-inspector
     ↑
eez-composer
```

## Tests

```bash
cargo test --package eez-evm-inspector
```

The tests cover proxy detection, dispatch behavior, and overlay state handling.
