#!/usr/bin/env bash
set -euo pipefail

BLOCK="${1:-44190}"
L2_RPC_URL="${L2_RPC_URL:-http://127.0.0.1:18688}"
CHAIN_CONFIG="${CHAIN_CONFIG:-/tmp/eez-failing-44190/l2-chainconfig.json}"
NATIVE_VALIDATE="${NATIVE_VALIDATE:-/home/edu/.cache/zisk-swap-build/target/release/native-validate}"
OUT_DIR="${OUT_DIR:-/tmp/eez-augmented-witness-$BLOCK}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

rpc() {
  local method="$1"
  local params="$2"
  curl -sS \
    -H 'content-type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$params}" \
    "$L2_RPC_URL"
}

need curl
need jq
need xxd

[[ -x "$NATIVE_VALIDATE" ]] || {
  echo "native validator is not executable: $NATIVE_VALIDATE" >&2
  exit 1
}
[[ -f "$CHAIN_CONFIG" ]] || {
  echo "chain config not found: $CHAIN_CONFIG" >&2
  exit 1
}

mkdir -p "$OUT_DIR"
block_hex="$(printf '0x%x' "$BLOCK")"

echo "L2 RPC:        $L2_RPC_URL"
echo "block:         $BLOCK ($block_hex)"
echo "out dir:       $OUT_DIR"
echo "chain config:  $CHAIN_CONFIG"

witness_response="$(rpc eez_executionWitnessAugmented "[\"$block_hex\"]")"
if jq -e '.error' >/dev/null <<<"$witness_response"; then
  echo "$witness_response" | jq '.' >&2
  exit 1
fi
jq '.result' <<<"$witness_response" >"$OUT_DIR/witness-$BLOCK.json"

raw_response="$(rpc debug_getRawBlock "[\"$block_hex\"]")"
if jq -e '.error' >/dev/null <<<"$raw_response"; then
  echo "$raw_response" | jq '.' >&2
  exit 1
fi
raw_hex="$(jq -r '.result' <<<"$raw_response")"
printf '%s' "${raw_hex#0x}" | xxd -r -p >"$OUT_DIR/block-$BLOCK.rlp"

echo "witness counts:"
jq '{state:(.state|length), keys:(.keys|length), codes:(.codes|length), headers:(.headers|length)}' \
  "$OUT_DIR/witness-$BLOCK.json"

"$NATIVE_VALIDATE" "$CHAIN_CONFIG" "$OUT_DIR/block-$BLOCK.rlp" "$OUT_DIR/witness-$BLOCK.json"
