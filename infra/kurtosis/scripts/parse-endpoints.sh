#!/usr/bin/env bash
set -euo pipefail

REPO="$(cd "$(dirname "$0")/../../.." && pwd)"
ENCLAVE="${KURTOSIS_ENCLAVE:-eez-devnet}"
OUT="${KURTOSIS_ENDPOINTS_FILE:-$REPO/infra/kurtosis/endpoints.env}"

command -v kurtosis >/dev/null || { echo "kurtosis not found" >&2; exit 1; }

INSPECT="$(kurtosis enclave inspect "$ENCLAVE")" || {
    echo "enclave '$ENCLAVE' not running" >&2
    exit 1
}

el_rpc="$(echo "$INSPECT" | grep -E 'el-1-.*rpc:' | head -1 | grep -oE 'http://127\.0\.0\.1:[0-9]+' || true)"
[[ -z "$el_rpc" ]] && el_rpc="$(echo "$INSPECT" | grep -E 'el-.*rpc:' | head -1 | grep -oE 'http://127\.0\.0\.1:[0-9]+' || true)"

builder_rpc="$(echo "$INSPECT" | grep -iE 'rbuilder|mev-builder' | grep -oE 'http://127\.0\.0\.1:[0-9]+' | head -1 || true)"
[[ -z "$builder_rpc" ]] && builder_rpc="$(echo "$INSPECT" | grep -iE 'mev-' | grep -oE 'http://127\.0\.0\.1:[0-9]+' | head -1 || true)"

[[ -n "$el_rpc" ]] || { echo "could not find EL RPC in inspect output" >&2; exit 1; }

cat >"$OUT" <<EOF
EEZ_L1_RPC_URL=$el_rpc
EEZ_L1_TARGET_RPC_URL=$el_rpc
EEZ_L1_BUILDER_RPC_URL=${builder_rpc:-http://127.0.0.1:37000}
EOF

echo "wrote $OUT"
cat "$OUT"
