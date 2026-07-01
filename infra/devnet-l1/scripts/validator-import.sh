#!/usr/bin/env bash
# One-shot: load ethereum-genesis-generator validator keystores into a
# Lighthouse VC datadir. Called from docker-compose.cl.yml (validator-import).
#
# Supports two layouts from ethpandaops/ethereum-genesis-generator:
#   A) keys/<0x-pubkey>/voting-keystore.json + secrets/<0x-pubkey>
#      -> copy into datadir for automatic validator discovery (preferred).
#   B) flat/nested keys under keys/ -> lighthouse account validator import
#      with an empty passphrase (generator default).
set -euo pipefail

KEYS_ROOT="${KEYS_ROOT:-/keys}"
TESTNET_DIR="${TESTNET_DIR:-/testnet}"
VC_DATADIR="${VC_DATADIR:-/vc-data}"

log() { echo "validator-import: $*"; }

if [ ! -d "$TESTNET_DIR" ]; then
    log "ERROR missing testnet dir $TESTNET_DIR"
    exit 1
fi

if [ ! -d "$KEYS_ROOT" ]; then
    log "ERROR missing keys root $KEYS_ROOT"
    exit 1
fi

log "keys root contents:"
ls -la "$KEYS_ROOT" || true

KEYS_DIR="$KEYS_ROOT/keys"
if [ ! -d "$KEYS_DIR" ]; then
    # Some generator versions write keystores directly under validator-keys/.
    if compgen -G "$KEYS_ROOT"/*/voting-keystore.json >/dev/null 2>&1; then
        KEYS_DIR="$KEYS_ROOT"
        log "using $KEYS_DIR (keys/ subdir absent; found voting-keystore.json here)"
    else
        log "ERROR expected $KEYS_ROOT/keys/ (or */voting-keystore.json under $KEYS_ROOT)"
        find "$KEYS_ROOT" -maxdepth 3 -type f 2>/dev/null | head -20 || true
        exit 1
    fi
fi

mkdir -p "$VC_DATADIR/validators" "$VC_DATADIR/secrets"

# Layout A: eth2-val-tools / genesis-generator key-centric tree.
if compgen -G "$KEYS_DIR"/*/voting-keystore.json >/dev/null 2>&1; then
    log "copying key-centric tree into $VC_DATADIR for auto-discovery"
    cp -a "$KEYS_DIR"/. "$VC_DATADIR/validators/"
    if [ -d "$KEYS_ROOT/secrets" ]; then
        cp -a "$KEYS_ROOT/secrets"/. "$VC_DATADIR/secrets/"
    else
        log "WARN no $KEYS_ROOT/secrets — assuming empty keystore passphrase"
        for dir in "$VC_DATADIR/validators"/*/; do
            [ -d "$dir" ] || continue
            pub="$(basename "$dir")"
            printf '\n' > "$VC_DATADIR/secrets/$pub"
        done
    fi
    log "done ($(find "$VC_DATADIR/validators" -name voting-keystore.json | wc -l) validators)"
    exit 0
fi

# Layout B: lighthouse import subcommand (needs passphrase on stdin).
log "no voting-keystore.json tree — falling back to lighthouse account import"
printf '\n' | lighthouse \
    --testnet-dir="$TESTNET_DIR" \
    account validator import \
    --directory="$KEYS_DIR" \
    --datadir="$VC_DATADIR" \
    --reuse-password \
    --stdin-inputs

log "import complete"
